package scheduler

import (
	"testing"
	"time"
)

func TestAdaptivePolicyLearnsRecurringLoadAndRejectsQuietSpike(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QuietConfirm = 10 * time.Minute
	cfg.RecoveryPeriod = 2 * time.Minute
	p := newAdaptivePolicy(cfg)
	start := time.Date(2026, time.August, 3, 0, 0, 0, 0, time.UTC) // Monday.

	var quiet, busy int
	for minute := range 15 * 24 * 60 {
		now := start.Add(time.Duration(minute) * time.Minute)
		hour := now.Hour()
		load := Load{RequestRate: 10, RateCV: 0.05, ReadP99: 500 * time.Microsecond}
		if hour >= 9 && hour < 19 {
			load.RequestRate = 1000
		}
		switch p.observe(now, load, false) {
		case StateQuiet:
			quiet++
		case StateBusy, StateOverloaded:
			busy++
		}
	}
	if quiet == 0 || busy == 0 {
		t.Fatalf("load states quiet=%d busy=%d", quiet, busy)
	}

	mondayMorning := start.Add(14*24*time.Hour + 10*time.Hour)
	expected, ready := p.seasonalExpected(mondayMorning)
	if !ready || expected < 500 {
		t.Fatalf("weekday seasonal baseline ready=%v expected=%.1f", ready, expected)
	}

	quietTime := start.Add(15*24*time.Hour + 2*time.Hour)
	spike := Load{RequestRate: 5, RateCV: 0.05, ReadP99: 20 * time.Millisecond}
	if got := p.observe(quietTime, spike, false); got != StateOverloaded {
		t.Fatalf("quiet-period p99 spike state = %s", got)
	}
}

func BenchmarkAdaptivePolicyTwoWeekLoad(b *testing.B) {
	cfg := DefaultConfig()
	start := time.Date(2026, time.August, 3, 0, 0, 0, 0, time.UTC)
	for b.Loop() {
		p := newAdaptivePolicy(cfg)
		for minute := range 14 * 24 * 60 {
			now := start.Add(time.Duration(minute) * time.Minute)
			rate := 10.0
			if now.Hour() >= 9 && now.Hour() < 19 {
				rate = 1000
			}
			p.observe(now, Load{RequestRate: rate, RateCV: 0.05, ReadP99: time.Millisecond}, false)
		}
	}
}
