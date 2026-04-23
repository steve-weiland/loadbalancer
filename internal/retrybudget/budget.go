// Package retrybudget implements the global retry budget described in
// spec §3.5 LB-44: at most `fraction` (default 0.10 = 10%) of total requests
// in any 1-second window may be retries.
//
// Implementation: a 100-slot ring buffer of (total, retry) counts, each slot
// covering 10ms of wall clock. Advances are amortized — each call to
// Observe / TryConsume rolls forward as many slots as time has elapsed since
// the last advance, zeroing them as we go. Reads sum across all slots to get
// the 1-second window totals. Constant work per call.
package retrybudget

import (
	"sync"
	"time"
)

const (
	slots         = 100
	slotDuration  = 10 * time.Millisecond
	windowSeconds = 1.0 // sum across all slots = 1s window
)

type Budget struct {
	fraction float64
	now      func() time.Time

	mu       sync.Mutex
	buckets  [slots]bucket
	head     int       // index of the most recent slot
	headTime time.Time // wall-clock start of the head slot

	exhausted uint64 // monotonic count of TryConsume() calls that returned false
}

type bucket struct {
	total int
	retry int
}

// New returns a Budget allowing `fraction` of total requests to be retries.
// Pass 0 ≤ fraction ≤ 1; New does not validate.
func New(fraction float64) *Budget {
	return &Budget{
		fraction: fraction,
		now:      time.Now,
		headTime: time.Now(),
	}
}

// withClock is a test seam.
func (b *Budget) withClock(now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = now
	b.headTime = now()
}

// Observe records one inbound request. Call once per *client* request (not
// per attempt). Used as the denominator in the budget calculation.
func (b *Budget) Observe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	b.buckets[b.head].total++
}

// TryConsume returns true iff issuing one more retry would keep the
// retry-to-total ratio at or below `fraction` over the trailing 1-second
// window. On true, the retry is recorded; on false, ExhaustedCount() is
// incremented and the caller MUST NOT retry.
func (b *Budget) TryConsume() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	total, retries := b.sum()
	// Allow the retry if (retries+1) / total ≤ fraction. total can be 0 if
	// somehow TryConsume is called before any Observe — in that case fall
	// back to denying (no traffic ⇒ no budget).
	if total == 0 {
		b.exhausted++
		return false
	}
	if float64(retries+1)/float64(total) > b.fraction {
		b.exhausted++
		return false
	}
	b.buckets[b.head].retry++
	return true
}

// ExhaustedCount returns the monotonic number of TryConsume calls that
// returned false. Used to drive lb_retry_budget_exhausted_total.
func (b *Budget) ExhaustedCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exhausted
}

// --- internal helpers (call with mu held) ---

// advance rolls the ring forward to "now", zeroing any newly-current slots.
func (b *Budget) advance() {
	now := b.now()
	elapsed := now.Sub(b.headTime)
	if elapsed < slotDuration {
		return
	}
	steps := int(elapsed / slotDuration)
	if steps >= slots {
		// Lost more than a window; reset entirely.
		for i := range b.buckets {
			b.buckets[i] = bucket{}
		}
		b.head = 0
		b.headTime = now
		return
	}
	for i := 0; i < steps; i++ {
		b.head = (b.head + 1) % slots
		b.buckets[b.head] = bucket{}
	}
	b.headTime = b.headTime.Add(time.Duration(steps) * slotDuration)
}

// sum returns the total and retry counts across the entire ring (1s window).
func (b *Budget) sum() (total, retries int) {
	for i := 0; i < slots; i++ {
		total += b.buckets[i].total
		retries += b.buckets[i].retry
	}
	return
}
