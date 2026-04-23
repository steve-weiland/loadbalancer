// Command chaos runs a vegeta load test against the load balancer while
// randomly killing and reviving backends on a fixed cadence. It writes a
// timestamped report directory containing:
//
//	vegeta.bin       raw vegeta results, replayable with `vegeta report`
//	timeseries.csv   per-second {ts, total, success, p50_ms, p99_ms} bins
//	chaos.log        timeline of kill/revive events
//	summary.txt      vegeta's default text report
//
// The runner spawns its own cluster (1 lbserver + N echobackends) so reports
// are reproducible — no port collisions with manually-started processes.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

type config struct {
	duration        time.Duration
	rps             int
	chaosInterval   time.Duration
	backends        int
	lbBin           string
	backendBin      string
	lbListen        string
	lbAdminListen   string
	backendBaseURL  string
	backendBasePort int
	reportRoot      string
	tag             string
	seed            int64
}

func main() {
	cfg := parseFlags()
	rng := rand.New(rand.NewSource(cfg.seed))

	reportDir := mkReportDir(cfg)
	fmt.Printf("report dir: %s\n", reportDir)

	cluster, err := startCluster(cfg)
	if err != nil {
		fail("start cluster: %v", err)
	}
	defer cluster.killAll()

	if err := waitReady(cfg, 10*time.Second); err != nil {
		fail("cluster did not become ready: %v", err)
	}
	fmt.Printf("cluster ready: lb=%s admin=%s backends=%d\n",
		cfg.lbListen, cfg.lbAdminListen, cfg.backends)

	chaosCtx, cancelChaos := context.WithCancel(context.Background())
	defer cancelChaos()

	chaosLog, err := os.Create(filepath.Join(reportDir, "chaos.log"))
	if err != nil {
		fail("open chaos.log: %v", err)
	}
	defer chaosLog.Close()

	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		runChaos(chaosCtx, cluster, cfg.chaosInterval, rng, chaosLog)
	}()

	fmt.Printf("attacking %s @ %d rps for %s\n", cfg.lbListen, cfg.rps, cfg.duration)
	results, metrics := attack(cfg)

	cancelChaos()
	chaosWG.Wait()

	if err := writeArtifacts(reportDir, results, metrics); err != nil {
		fail("write artifacts: %v", err)
	}

	fmt.Println()
	fmt.Println(string(textReport(metrics)))
	fmt.Printf("\nartifacts in %s\n", reportDir)
}

func parseFlags() config {
	cfg := config{}
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "attack duration")
	flag.IntVar(&cfg.rps, "rps", 200, "requests per second")
	flag.DurationVar(&cfg.chaosInterval, "chaos-interval", 10*time.Second, "kill/revive cadence")
	flag.IntVar(&cfg.backends, "backends", 3, "number of echo backends to spawn")
	flag.StringVar(&cfg.lbBin, "lb-bin", "./bin/lbserver", "path to lbserver binary")
	flag.StringVar(&cfg.backendBin, "backend-bin", "./bin/echobackend", "path to echobackend binary")
	flag.StringVar(&cfg.lbListen, "lb-listen", ":7080", "lb proxy listen address")
	flag.StringVar(&cfg.lbAdminListen, "lb-admin-listen", ":7090", "lb admin listen address")
	flag.IntVar(&cfg.backendBasePort, "backend-base-port", 9001, "first backend port (others increment)")
	flag.StringVar(&cfg.reportRoot, "report-root", "reports", "directory under which timestamped report dirs are created")
	flag.StringVar(&cfg.tag, "tag", "v1", "report tag (e.g. v1, v2)")
	flag.Int64Var(&cfg.seed, "seed", time.Now().UnixNano(), "RNG seed for kill/revive choices")
	flag.Parse()
	return cfg
}

func mkReportDir(cfg config) string {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	dir := filepath.Join(cfg.reportRoot, fmt.Sprintf("%s-%s", cfg.tag, stamp))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail("mkdir report dir: %v", err)
	}
	return dir
}

// ---------- cluster management ----------

type backendProc struct {
	id   string
	port int
	url  string
	cmd  *exec.Cmd
	live bool
}

type cluster struct {
	mu       sync.Mutex
	lb       *exec.Cmd
	backends []*backendProc
	cfg      config
}

func startCluster(cfg config) (*cluster, error) {
	c := &cluster{cfg: cfg}

	for i := 0; i < cfg.backends; i++ {
		port := cfg.backendBasePort + i
		id := fmt.Sprintf("b%d", i+1)
		bp := &backendProc{
			id:   id,
			port: port,
			url:  fmt.Sprintf("http://localhost:%d", port),
		}
		if err := c.spawnBackend(bp); err != nil {
			c.killAll()
			return nil, fmt.Errorf("spawn %s: %w", id, err)
		}
		c.backends = append(c.backends, bp)
	}

	backendCSV := ""
	for i, bp := range c.backends {
		if i > 0 {
			backendCSV += ","
		}
		backendCSV += bp.url
	}

	lb := exec.Command(cfg.lbBin,
		"--listen="+cfg.lbListen,
		"--admin-listen="+cfg.lbAdminListen,
		"--backends="+backendCSV,
	)
	lb.Stdout = mustCreate(filepath.Join(os.TempDir(), "chaos-lbserver.log"))
	lb.Stderr = lb.Stdout
	if err := lb.Start(); err != nil {
		c.killAll()
		return nil, fmt.Errorf("start lbserver: %w", err)
	}
	c.lb = lb
	return c, nil
}

func (c *cluster) spawnBackend(bp *backendProc) error {
	cmd := exec.Command(c.cfg.backendBin,
		fmt.Sprintf("--listen=:%d", bp.port),
		"--id="+bp.id,
	)
	logPath := filepath.Join(os.TempDir(), fmt.Sprintf("chaos-%s.log", bp.id))
	cmd.Stdout = mustCreate(logPath)
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}
	bp.cmd = cmd
	bp.live = true
	return nil
}

func (c *cluster) killAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lb != nil && c.lb.Process != nil {
		_ = c.lb.Process.Signal(syscall.SIGTERM)
	}
	for _, bp := range c.backends {
		if bp.cmd != nil && bp.cmd.Process != nil && bp.live {
			_ = bp.cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	if c.lb != nil {
		_ = c.lb.Wait()
	}
	for _, bp := range c.backends {
		if bp.cmd != nil {
			_ = bp.cmd.Wait()
		}
	}
}

func waitReady(cfg config, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	healthURL := "http://localhost" + cfg.lbAdminListen + "/healthz"
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timeout waiting for /healthz")
}

// ---------- chaos schedule ----------

func runChaos(ctx context.Context, c *cluster, interval time.Duration, rng *rand.Rand, log *os.File) {
	logEvent := func(action, id string, err error) {
		ts := time.Now().UTC().Format(time.RFC3339Nano)
		msg := fmt.Sprintf("%s\t%s\t%s", ts, action, id)
		if err != nil {
			msg += "\terr=" + err.Error()
		}
		fmt.Fprintln(log, msg)
		fmt.Println("[chaos]", msg)
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.mu.Lock()
			liveIdxs := []int{}
			deadIdxs := []int{}
			for i, bp := range c.backends {
				if bp.live {
					liveIdxs = append(liveIdxs, i)
				} else {
					deadIdxs = append(deadIdxs, i)
				}
			}
			c.mu.Unlock()

			// Decide: kill or revive. Revive if all are dead-able protected
			// (always keep at least 1 alive to avoid total outage being
			// uninteresting); revive if some are dead and rng says so.
			killable := len(liveIdxs) > 1
			reviveable := len(deadIdxs) > 0

			var doKill bool
			switch {
			case killable && reviveable:
				doKill = rng.Intn(2) == 0
			case killable:
				doKill = true
			case reviveable:
				doKill = false
			default:
				continue // 1 alive, 0 dead — leave it alone
			}

			c.mu.Lock()
			if doKill {
				idx := liveIdxs[rng.Intn(len(liveIdxs))]
				bp := c.backends[idx]
				err := bp.cmd.Process.Signal(syscall.SIGTERM)
				if err == nil {
					_ = bp.cmd.Wait()
				}
				bp.live = false
				bp.cmd = nil
				c.mu.Unlock()
				logEvent("KILL", bp.id, err)
			} else {
				idx := deadIdxs[rng.Intn(len(deadIdxs))]
				bp := c.backends[idx]
				err := c.spawnBackend(bp)
				c.mu.Unlock()
				logEvent("REVIVE", bp.id, err)
			}
		}
	}
}

// ---------- vegeta attack + binning ----------

type secondBin struct {
	ts        time.Time
	total     int
	success   int
	latencies []time.Duration
}

func attack(cfg config) ([]vegeta.Result, *vegeta.Metrics) {
	rate := vegeta.Rate{Freq: cfg.rps, Per: time.Second}
	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: "GET",
		URL:    "http://localhost" + cfg.lbListen + "/anything",
	})

	var (
		all []vegeta.Result
		m   vegeta.Metrics
	)
	for res := range vegeta.NewAttacker().Attack(targeter, rate, cfg.duration, "lb-chaos") {
		all = append(all, *res)
		m.Add(res)
	}
	m.Close()
	return all, &m
}

func binBySecond(results []vegeta.Result) []secondBin {
	if len(results) == 0 {
		return nil
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.Before(results[j].Timestamp)
	})
	start := results[0].Timestamp.Truncate(time.Second)
	binsByTS := map[int64]*secondBin{}
	order := []int64{}
	for _, r := range results {
		ts := r.Timestamp.Truncate(time.Second)
		key := ts.Unix()
		b, ok := binsByTS[key]
		if !ok {
			b = &secondBin{ts: ts}
			binsByTS[key] = b
			order = append(order, key)
		}
		b.total++
		if r.Code >= 200 && r.Code < 400 && r.Error == "" {
			b.success++
		}
		b.latencies = append(b.latencies, r.Latency)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := make([]secondBin, 0, len(order))
	for _, k := range order {
		out = append(out, *binsByTS[k])
	}
	_ = start
	return out
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// ---------- artifact writers ----------

func writeArtifacts(dir string, results []vegeta.Result, m *vegeta.Metrics) error {
	if err := writeVegetaBin(filepath.Join(dir, "vegeta.bin"), results); err != nil {
		return fmt.Errorf("vegeta.bin: %w", err)
	}
	if err := writeTimeseriesCSV(filepath.Join(dir, "timeseries.csv"), results); err != nil {
		return fmt.Errorf("timeseries.csv: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.txt"), textReport(m), 0o644); err != nil {
		return fmt.Errorf("summary.txt: %w", err)
	}
	return nil
}

func writeVegetaBin(path string, results []vegeta.Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := vegeta.NewEncoder(f)
	for i := range results {
		if err := enc.Encode(&results[i]); err != nil {
			return err
		}
	}
	return nil
}

func writeTimeseriesCSV(path string, results []vegeta.Result) error {
	bins := binBySecond(results)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"ts_unix", "ts_iso", "total", "success", "success_rate", "p50_ms", "p99_ms"}); err != nil {
		return err
	}
	for _, b := range bins {
		sort.Slice(b.latencies, func(i, j int) bool { return b.latencies[i] < b.latencies[j] })
		p50 := percentile(b.latencies, 0.50)
		p99 := percentile(b.latencies, 0.99)
		successRate := 0.0
		if b.total > 0 {
			successRate = float64(b.success) / float64(b.total)
		}
		row := []string{
			strconv.FormatInt(b.ts.Unix(), 10),
			b.ts.UTC().Format(time.RFC3339),
			strconv.Itoa(b.total),
			strconv.Itoa(b.success),
			fmt.Sprintf("%.4f", successRate),
			fmt.Sprintf("%.3f", float64(p50.Microseconds())/1000.0),
			fmt.Sprintf("%.3f", float64(p99.Microseconds())/1000.0),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func textReport(m *vegeta.Metrics) []byte {
	return []byte(fmt.Sprintf(
		"Requests      [total]              %d\n"+
			"Duration      [attack, wait]       %s, %s\n"+
			"Latencies     [min, mean, p50, p90, p95, p99, max]  %s, %s, %s, %s, %s, %s, %s\n"+
			"Bytes In      [total, mean]        %d, %.2f\n"+
			"Bytes Out     [total, mean]        %d, %.2f\n"+
			"Success       [ratio]              %.2f%%\n"+
			"Status Codes  [code:count]         %v\n"+
			"Error Set:\n%s\n",
		m.Requests,
		m.Duration, m.Wait,
		m.Latencies.Min, m.Latencies.Mean, m.Latencies.P50, m.Latencies.P90, m.Latencies.P95, m.Latencies.P99, m.Latencies.Max,
		m.BytesIn.Total, m.BytesIn.Mean,
		m.BytesOut.Total, m.BytesOut.Mean,
		m.Success*100,
		m.StatusCodes,
		errSet(m.Errors),
	))
}

func errSet(errs []string) string {
	if len(errs) == 0 {
		return "  (none)"
	}
	out := ""
	for _, e := range errs {
		out += "  " + e + "\n"
	}
	return out
}

// ---------- helpers ----------

func mustCreate(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		fail("create %s: %v", path, err)
	}
	return f
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "chaos: "+format+"\n", args...)
	os.Exit(1)
}
