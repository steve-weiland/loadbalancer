package backend

import (
	"net/url"
	"testing"
	"time"

	"github.com/steve-weiland/loadbalancer/internal/breaker"
	"github.com/steve-weiland/loadbalancer/internal/ewma"
)

func mkBackend(t *testing.T) *Backend {
	t.Helper()
	u, err := url.Parse("http://example.test:1234")
	if err != nil {
		t.Fatal(err)
	}
	e := ewma.New(0.1, time.Second)
	br := breaker.New(breaker.Config{
		Window:         4,
		ErrorThreshold: 0.5,
		ResetTimeout:   10 * time.Millisecond,
		ResetCap:       100 * time.Millisecond,
	})
	return New(u, e, br)
}

func TestNew_PanicsOnNilDependency(t *testing.T) {
	cases := []struct {
		u  *url.URL
		e  *ewma.Score
		br *breaker.Breaker
	}{
		{nil, ewma.New(0.1, time.Second), breaker.New(breaker.Config{Window: 1, ResetTimeout: time.Millisecond, ResetCap: time.Millisecond})},
		{&url.URL{}, nil, breaker.New(breaker.Config{Window: 1, ResetTimeout: time.Millisecond, ResetCap: time.Millisecond})},
		{&url.URL{}, ewma.New(0.1, time.Second), nil},
	}
	for i, tc := range cases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("case %d: want panic on nil dep", i)
				}
			}()
			_ = New(tc.u, tc.e, tc.br)
		}()
	}
}

func TestEligibility_TracksBreakerState(t *testing.T) {
	b := mkBackend(t)
	if got := b.Eligibility(); got != EligiblePrimary {
		t.Errorf("fresh backend Eligibility = %v, want EligiblePrimary", got)
	}
	// Trip the breaker (window=4, threshold=0.5 → 3 errors trips).
	for i := 0; i < 4; i++ {
		_ = b.Breaker.Allow()
		b.Breaker.Record(false)
	}
	if got := b.Eligibility(); got != IneligibleOpen {
		t.Errorf("tripped backend Eligibility = %v, want IneligibleOpen", got)
	}
}

func TestString_ReturnsURL(t *testing.T) {
	b := mkBackend(t)
	if got, want := b.String(), "http://example.test:1234"; got != want {
		t.Errorf("String()=%q, want %q", got, want)
	}
}
