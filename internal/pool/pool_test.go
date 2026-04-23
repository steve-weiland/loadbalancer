package pool

import (
	"errors"
	"sync"
	"testing"
)

func TestNew_EmptyPoolErrors(t *testing.T) {
	_, err := New(nil)
	if !errors.Is(err, ErrEmptyPool) {
		t.Fatalf("want ErrEmptyPool, got %v", err)
	}
	_, err = New([]string{})
	if !errors.Is(err, ErrEmptyPool) {
		t.Fatalf("want ErrEmptyPool, got %v", err)
	}
}

func TestNew_RejectsMalformedURL(t *testing.T) {
	cases := []string{"not-a-url", "://broken", "host-only-no-scheme"}
	for _, c := range cases {
		if _, err := New([]string{c}); err == nil {
			t.Errorf("want error for %q, got nil", c)
		}
	}
}

func TestRoundRobin_FairnessOver1000Requests(t *testing.T) {
	p, err := New([]string{
		"http://a:1", "http://b:2", "http://c:3",
	})
	if err != nil {
		t.Fatal(err)
	}
	const total = 1002 // exact multiple of 3 → expect 334 each
	counts := map[string]int{}
	for i := 0; i < total; i++ {
		counts[p.Next().Host]++
	}
	for _, host := range []string{"a:1", "b:2", "c:3"} {
		got := counts[host]
		if got != total/3 {
			t.Errorf("backend %s: got %d hits, want %d", host, got, total/3)
		}
	}
}

func TestRoundRobin_OrderIsDeterministicFromCounter(t *testing.T) {
	p, err := New([]string{"http://a:1", "http://b:2", "http://c:3"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a:1", "b:2", "c:3", "a:1", "b:2", "c:3"}
	for i, w := range want {
		if got := p.Next().Host; got != w {
			t.Errorf("call %d: got %s, want %s", i, got, w)
		}
	}
}

// Run with `go test -race` to verify LB-02's atomic correctness.
func TestRoundRobin_ConcurrentNextNoRace(t *testing.T) {
	p, err := New([]string{"http://a:1", "http://b:2", "http://c:3", "http://d:4"})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 32
	const perG = 5_000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				_ = p.Next()
			}
		}()
	}
	wg.Wait()
	// Total dispatch count is well-defined under atomic.Add even if per-backend
	// distribution can skew slightly under contention. We assert no deadlock,
	// no panic, and every backend got *some* traffic.
	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		counts[p.Next().Host]++ // +4 more
	}
	if len(counts) != 4 {
		t.Errorf("expected each of 4 backends to be selected, got %d", len(counts))
	}
}

func TestBackends_ReturnsCopy(t *testing.T) {
	p, err := New([]string{"http://a:1", "http://b:2"})
	if err != nil {
		t.Fatal(err)
	}
	got := p.Backends()
	if len(got) != 2 {
		t.Fatalf("want 2 backends, got %d", len(got))
	}
	got[0] = nil // mutating caller's slice must not affect pool
	if p.Backends()[0] == nil {
		t.Error("Backends() returned the underlying slice; want a copy")
	}
}
