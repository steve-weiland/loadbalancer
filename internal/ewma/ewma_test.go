package ewma

import (
	"math"
	"sync"
	"testing"
	"time"
)

const tol = 1e-9

func approx(t *testing.T, got, want float64, msg string) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s: got %.9f, want %.9f", msg, got, want)
	}
}

func TestNew_PanicsOnBadAlpha(t *testing.T) {
	for _, a := range []float64{-0.1, 0, 1.0001, 2} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("alpha=%v: want panic, got none", a)
				}
			}()
			_ = New(a, time.Second)
		}()
	}
}

func TestNew_PanicsOnBadDecay(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("decay=%v: want panic, got none", d)
				}
			}()
			_ = New(0.1, d)
		}()
	}
}

func TestUpdate_FirstObservationSeedsWithoutSmoothing(t *testing.T) {
	s := New(0.1, time.Second)
	s.Update(100 * time.Millisecond)
	got := s.Value()
	if d := abs(got - 100*time.Millisecond); d > 100*time.Microsecond {
		t.Errorf("first observation should seed; got %v want ~100ms", got)
	}
}

func TestUpdate_SecondObservationSmooths(t *testing.T) {
	s := New(0.1, time.Hour) // long decay → ignore decay in this test
	s.Update(100 * time.Millisecond)
	s.Update(200 * time.Millisecond)
	// 0.1*200 + 0.9*100 = 110ms
	got := s.Value()
	want := 110 * time.Millisecond
	if d := abs(got - want); d > time.Millisecond {
		t.Errorf("smoothing: got %v want %v", got, want)
	}
}

func TestValue_IdleDecayHalvesPerHalfLife(t *testing.T) {
	// Use a long half-life so a few microseconds of test runtime are negligible
	// against the simulated idle period. Without this, time.Since(lastObs) picks
	// up the elapsed test execution time and skews the assertion.
	s := New(0.1, 10*time.Second)
	s.Update(100 * time.Millisecond)
	// Force lastObs into the past to simulate idle.
	s.mu.Lock()
	s.lastObs = time.Now().Add(-30 * time.Second) // 3 half-lives → /8
	s.mu.Unlock()
	got := s.Value().Seconds()
	want := 0.100 / 8
	if rel := math.Abs(got-want) / want; rel > 0.001 {
		t.Errorf("decay 3×half-life: got %.6f, want ~%.6f (rel err %.4f)", got, want, rel)
	}
}

func TestValue_NotSeededReturnsZero(t *testing.T) {
	s := New(0.1, time.Second)
	if v := s.Value(); v != 0 {
		t.Errorf("unseeded Value = %v, want 0", v)
	}
}

func TestSeeded_ReportsState(t *testing.T) {
	s := New(0.1, time.Second)
	if s.Seeded() {
		t.Error("new score should report Seeded()=false")
	}
	s.Update(time.Millisecond)
	if !s.Seeded() {
		t.Error("after Update, Seeded()=true expected")
	}
}

// Run with -race; verifies the mutex isn't dropped.
func TestConcurrentUpdateAndValueNoRace(t *testing.T) {
	s := New(0.1, time.Second)
	const goroutines = 16
	const perG = 5_000
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				s.Update(time.Microsecond * time.Duration(j%1000))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				_ = s.Value()
			}
		}()
	}
	wg.Wait()
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
