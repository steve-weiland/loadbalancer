package retrybudget

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

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

func TestObserveAndConsume_HappyPath(t *testing.T) {
	b := New(0.1) // 10%
	for i := 0; i < 100; i++ {
		b.Observe()
	}
	// 100 total, 10% budget → 10 retries allowed.
	for i := 0; i < 10; i++ {
		if !b.TryConsume() {
			t.Fatalf("retry %d: budget should allow", i+1)
		}
	}
	if b.TryConsume() {
		t.Error("retry 11: budget should refuse (would exceed 10%)")
	}
	if got := b.ExhaustedCount(); got != 1 {
		t.Errorf("ExhaustedCount=%d, want 1", got)
	}
}

func TestTryConsume_DeniesWhenZeroTraffic(t *testing.T) {
	b := New(0.5)
	if b.TryConsume() {
		t.Error("TryConsume with zero observed traffic should return false")
	}
}

func TestZeroBudget_AlwaysDenies(t *testing.T) {
	b := New(0)
	for i := 0; i < 100; i++ {
		b.Observe()
	}
	if b.TryConsume() {
		t.Error("budget=0 should always deny")
	}
}

func TestFullBudget_AlwaysAllows(t *testing.T) {
	b := New(1.0)
	for i := 0; i < 10; i++ {
		b.Observe()
	}
	for i := 0; i < 10; i++ {
		if !b.TryConsume() {
			t.Errorf("budget=1.0 should allow retry %d", i)
		}
	}
}

func TestSlidingWindow_DropsOldEntries(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	b := New(0.1)
	b.withClock(clk.Now)

	// 100 requests at t=0.
	for i := 0; i < 100; i++ {
		b.Observe()
	}
	// Burn the budget.
	for i := 0; i < 10; i++ {
		if !b.TryConsume() {
			t.Fatal("budget should allow")
		}
	}
	if b.TryConsume() {
		t.Fatal("budget should be exhausted")
	}
	// Advance past the 1s window — old observations and retries fall off.
	clk.Advance(2 * time.Second)
	// Add new traffic.
	for i := 0; i < 100; i++ {
		b.Observe()
	}
	// Budget refreshed; should allow 10 more.
	for i := 0; i < 10; i++ {
		if !b.TryConsume() {
			t.Errorf("after window slide, retry %d should be allowed", i+1)
		}
	}
}

func TestPartialWindowSlide_BehavesGracefully(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	b := New(0.1)
	b.withClock(clk.Now)

	// 50 reqs at t=0.
	for i := 0; i < 50; i++ {
		b.Observe()
	}
	// Slide forward 500ms (half window).
	clk.Advance(500 * time.Millisecond)
	// 50 more reqs at t=0.5s. Total in window now = 100.
	for i := 0; i < 50; i++ {
		b.Observe()
	}
	// Budget = 10% of 100 = 10 retries.
	allowed := 0
	for i := 0; i < 20; i++ {
		if b.TryConsume() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("partial-window: allowed=%d, want 10", allowed)
	}
}

// Run with -race; concurrent Observe and TryConsume across many goroutines
// must not race. We don't assert exact totals (concurrent allowance is
// inherently noisy at the boundary), only "no race, no panic, exhausted ≥ 0".
func TestConcurrent_NoRace(t *testing.T) {
	b := New(0.1)
	var wg sync.WaitGroup
	const goroutines = 16
	const perG = 1_000
	var consumed atomic.Int64
	for i := 0; i < goroutines; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				b.Observe()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				if b.TryConsume() {
					consumed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
}
