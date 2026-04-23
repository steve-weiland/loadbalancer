package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func defaultCfg() Config {
	return Config{
		Window:         10,
		ErrorThreshold: 0.5,
		ResetTimeout:   100 * time.Millisecond,
		ResetCap:       1 * time.Second,
	}
}

// fakeClock is a minimal mockable time source.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(start time.Time) *fakeClock { return &fakeClock{t: start} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestStartsClosed(t *testing.T) {
	b := New(defaultCfg())
	if got := b.State(); got != Closed {
		t.Errorf("initial state = %v, want Closed", got)
	}
}

func TestClosed_AllowAlwaysTrue(t *testing.T) {
	b := New(defaultCfg())
	for i := 0; i < 100; i++ {
		if !b.Allow() {
			t.Fatal("Closed breaker must always Allow")
		}
		b.Record(true)
	}
}

func TestClosed_TripsOnErrorRateAboveThreshold(t *testing.T) {
	b := New(defaultCfg()) // window=10, threshold=0.5
	// 5 successes + 6 errors = 6/10 errors after the window fills... wait no.
	// 5 successes then 6 errors = 11 records, last 10 are 4 success + 6 error = 0.6 > 0.5.
	for i := 0; i < 5; i++ {
		_ = b.Allow()
		b.Record(true)
	}
	for i := 0; i < 6; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	if got := b.State(); got != Open {
		t.Errorf("state = %v, want Open after 6/10 errors", got)
	}
}

func TestClosed_DoesNotTripOnExactlyThreshold(t *testing.T) {
	b := New(defaultCfg()) // threshold=0.5; 5/10 errors == exactly threshold, NOT >
	for i := 0; i < 5; i++ {
		_ = b.Allow()
		b.Record(true)
	}
	for i := 0; i < 5; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	if got := b.State(); got != Closed {
		t.Errorf("state = %v, want Closed at exactly threshold", got)
	}
}

func TestClosed_DoesNotTripUntilWindowFilled(t *testing.T) {
	b := New(defaultCfg()) // window=10
	// 4 errors out of 4 records: error rate = 1.0 but window not full → no trip.
	for i := 0; i < 4; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	if got := b.State(); got != Closed {
		t.Errorf("state = %v, want Closed (window not yet filled)", got)
	}
}

func TestOpen_RejectsUntilTimeout(t *testing.T) {
	clk := newClock(time.Unix(0, 0))
	b := New(defaultCfg(), WithClock(clk.Now))
	// Trip it.
	for i := 0; i < 10; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	if got := b.State(); got != Open {
		t.Fatalf("setup failed: state %v", got)
	}
	// Before timeout: no Allow.
	clk.Advance(50 * time.Millisecond)
	if b.Allow() {
		t.Error("Allow returned true before reset timeout")
	}
	// After timeout: Allow as probe → state Half-open.
	clk.Advance(60 * time.Millisecond) // total 110ms > 100ms
	if !b.Allow() {
		t.Fatal("Allow returned false after reset timeout")
	}
	if got := b.State(); got != HalfOpen {
		t.Errorf("state after probe-allow = %v, want HalfOpen", got)
	}
}

func TestHalfOpen_OneProbeAtATime(t *testing.T) {
	clk := newClock(time.Unix(0, 0))
	b := New(defaultCfg(), WithClock(clk.Now))
	for i := 0; i < 10; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	clk.Advance(200 * time.Millisecond) // past reset timeout
	// First Allow becomes the probe → HalfOpen with halfOpenInUse=true.
	if !b.Allow() {
		t.Fatal("first probe denied")
	}
	// Second concurrent Allow must be denied.
	if b.Allow() {
		t.Error("second probe Allow returned true while first probe outstanding")
	}
}

func TestHalfOpen_SuccessReclosesAndResetsTimeout(t *testing.T) {
	clk := newClock(time.Unix(0, 0))
	b := New(defaultCfg(), WithClock(clk.Now))
	for i := 0; i < 10; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	clk.Advance(200 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("probe denied")
	}
	b.Record(true) // probe success
	if got := b.State(); got != Closed {
		t.Errorf("state after success probe = %v, want Closed", got)
	}
	// Verify currentTimeout is back to base by re-tripping and checking the new
	// open-window length: trip again → Open → must wait full ResetTimeout, not
	// a doubled value.
	for i := 0; i < 10; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	clk.Advance(50 * time.Millisecond)
	if b.Allow() {
		t.Error("after success-then-retrip, Allow returned true before base timeout")
	}
	clk.Advance(60 * time.Millisecond)
	if !b.Allow() {
		t.Error("after success-then-retrip, Allow stayed false past base timeout — timeout was not reset")
	}
}

func TestHalfOpen_FailureReopensWithDoubledTimeoutCapped(t *testing.T) {
	clk := newClock(time.Unix(0, 0))
	cfg := Config{Window: 4, ErrorThreshold: 0.5, ResetTimeout: 50 * time.Millisecond, ResetCap: 200 * time.Millisecond}
	b := New(cfg, WithClock(clk.Now))
	trip := func() {
		for i := 0; i < 4; i++ {
			_ = b.Allow()
			b.Record(false)
		}
	}
	probeFail := func() {
		if !b.Allow() {
			t.Fatal("probe denied")
		}
		b.Record(false)
	}

	trip()
	clk.Advance(60 * time.Millisecond) // > 50ms initial
	probeFail()                        // → Open, timeout doubled to 100ms

	// Verify 100ms timeout: 80ms denied, 110ms admitted.
	clk.Advance(80 * time.Millisecond)
	if b.Allow() {
		t.Error("Allow at 80ms should be denied (doubled timeout = 100ms)")
	}
	clk.Advance(30 * time.Millisecond) // total 110ms
	if !b.Allow() {
		t.Fatal("probe denied at 110ms past doubled-timeout reopen")
	}
	b.Record(false) // → Open, timeout doubles to 200ms (== cap)

	// Verify cap holds: 220ms is just past cap.
	clk.Advance(220 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("probe denied at cap+20ms")
	}
	b.Record(false) // → Open, timeout would double to 400ms but capped at 200ms

	clk.Advance(220 * time.Millisecond)
	if !b.Allow() {
		t.Error("timeout exceeded ResetCap — doubling was not clamped")
	}
}

func TestOnTransitionFires(t *testing.T) {
	type tx struct{ from, to State }
	var txs []tx
	var mu sync.Mutex
	notif := func(from, to State) {
		mu.Lock()
		defer mu.Unlock()
		txs = append(txs, tx{from, to})
	}
	clk := newClock(time.Unix(0, 0))
	b := New(defaultCfg(), WithClock(clk.Now), WithOnTransition(notif))
	for i := 0; i < 10; i++ {
		_ = b.Allow()
		b.Record(false)
	}
	clk.Advance(200 * time.Millisecond)
	_ = b.Allow()  // → HalfOpen
	b.Record(true) // → Closed

	mu.Lock()
	defer mu.Unlock()
	want := []tx{{Closed, Open}, {Open, HalfOpen}, {HalfOpen, Closed}}
	if len(txs) != len(want) {
		t.Fatalf("transitions: got %v want %v", txs, want)
	}
	for i := range want {
		if txs[i] != want[i] {
			t.Errorf("transition %d: got %v want %v", i, txs[i], want[i])
		}
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{Closed: "closed", Open: "open", HalfOpen: "half-open", State(99): "unknown"}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("%d.String()=%q want %q", s, got, want)
		}
	}
}

// Run with -race; concurrent Allow + Record across many goroutines must not
// race. Outcome semantics aren't asserted beyond "no data race, no panic".
func TestConcurrent_NoRace(t *testing.T) {
	b := New(defaultCfg())
	var wg sync.WaitGroup
	const goroutines = 16
	const perG = 1_000
	var allowed atomic.Int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				if b.Allow() {
					allowed.Add(1)
					b.Record((seed+j)%3 != 0) // ~67% success
				}
			}
		}(i)
	}
	wg.Wait()
	_ = allowed.Load()
}
