package scheduler

import "time"

func classAllowed(class Class, state State) bool {
	switch class {
	case ClassMaintenance:
		return state == StateNormal || state == StateQuietCandidate || state == StateQuiet
	case ClassDictionaryTrain, ClassDictionaryEvaluate, ClassSchedulerModelPersist:
		return state == StateQuiet
	default:
		return false
	}
}

func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	if cfg.ObservationWindow <= 0 {
		cfg.ObservationWindow = defaults.ObservationWindow
	}
	if cfg.QuietConfirm <= 0 {
		cfg.QuietConfirm = defaults.QuietConfirm
	}
	if cfg.RecoveryPeriod <= 0 {
		cfg.RecoveryPeriod = defaults.RecoveryPeriod
	}
	if cfg.TargetReadP99 <= 0 {
		cfg.TargetReadP99 = defaults.TargetReadP99
	}
	if cfg.TargetWriteP99 <= 0 {
		cfg.TargetWriteP99 = defaults.TargetWriteP99
	}
	if cfg.QuietRateCeiling <= 0 {
		cfg.QuietRateCeiling = defaults.QuietRateCeiling
	}
	if cfg.QuietRateRatio <= 0 {
		cfg.QuietRateRatio = defaults.QuietRateRatio
	}
	if cfg.BusyRateRatio <= 0 {
		cfg.BusyRateRatio = defaults.BusyRateRatio
	}
	if cfg.QuietHeadroom <= 0 {
		cfg.QuietHeadroom = defaults.QuietHeadroom
	}
	if cfg.QuietMaxRateCV <= 0 {
		cfg.QuietMaxRateCV = defaults.QuietMaxRateCV
	}
	if cfg.OverloadRatio <= 0 {
		cfg.OverloadRatio = defaults.OverloadRatio
	}
	if cfg.MaxCPUUtilization <= 0 {
		cfg.MaxCPUUtilization = defaults.MaxCPUUtilization
	}
	if cfg.MaxDiskUtilization <= 0 {
		cfg.MaxDiskUtilization = defaults.MaxDiskUtilization
	}
	if cfg.MaxMemoryUtilization <= 0 {
		cfg.MaxMemoryUtilization = defaults.MaxMemoryUtilization
	}
	return cfg
}

func resourcesAllow(cost Cost, load Load, cfg Config) bool {
	return load.CPUUtilization+max(0, cost.CPUFraction) <= cfg.MaxCPUUtilization &&
		load.DiskUtilization+max(0, cost.DiskFraction) <= cfg.MaxDiskUtilization &&
		load.MemoryUtilization+max(0, cost.MemoryFraction) <= cfg.MaxMemoryUtilization
}

type adaptivePolicy struct {
	cfg          Config
	modelChanged func(uint64)
	generation   uint64

	state          State
	baselineRate   float64
	baselineReady  bool
	baselinePoints int
	seasonal       [7 * 24 * 4]seasonalPoint
	seasonalWindow seasonalWindow
	expectedRate   float64
	quietSince     time.Time
	pressureEnded  time.Time
}

type seasonalPoint struct {
	rate    float64
	samples uint32
}

type seasonalWindow struct {
	active bool
	key    int64
	index  int
	sum    float64
	count  uint64
}

func newAdaptivePolicy(cfg Config) *adaptivePolicy {
	return &adaptivePolicy{cfg: normalizeConfig(cfg), modelChanged: cfg.ModelChanged, state: StateNormal}
}

func (p *adaptivePolicy) observe(now time.Time, load Load, backgroundActive bool) State {
	if !backgroundActive {
		p.updateBaseline(load.RequestRate)
		if p.updateSeasonal(now, load.RequestRate) {
			p.generation++
			if p.modelChanged != nil {
				p.modelChanged(p.generation)
			}
		}
	}
	expected, seasonalReady := p.seasonalExpected(now)
	if !seasonalReady {
		expected = p.baselineRate
	}
	p.expectedRate = expected
	latencyRatio := maxRatio(load.ReadP99, p.cfg.TargetReadP99, load.WriteP99, p.cfg.TargetWriteP99)
	rateRatio := 0.0
	if expected > 0 {
		rateRatio = load.RequestRate / expected
	}

	switch {
	case latencyRatio >= p.cfg.OverloadRatio:
		p.quietSince = time.Time{}
		p.pressureEnded = time.Time{}
		p.state = StateOverloaded
		return p.state
	case latencyRatio >= 1 || (p.baselineReady && rateRatio >= p.cfg.BusyRateRatio):
		p.quietSince = time.Time{}
		p.pressureEnded = time.Time{}
		p.state = StateBusy
		return p.state
	}

	if p.state == StateBusy || p.state == StateOverloaded || p.state == StateRecovering {
		if p.pressureEnded.IsZero() {
			p.pressureEnded = now
		}
		if now.Sub(p.pressureEnded) < p.cfg.RecoveryPeriod {
			p.state = StateRecovering
			return p.state
		}
	}
	p.pressureEnded = time.Time{}

	quietRate := load.RequestRate <= p.cfg.QuietRateCeiling
	if p.baselineReady && load.RequestRate <= p.baselineRate*p.cfg.QuietRateRatio {
		quietRate = true
	}
	quietLatency := latencyRatio == 0 || latencyRatio <= p.cfg.QuietHeadroom
	quietStable := load.RateCV <= p.cfg.QuietMaxRateCV
	if quietRate && quietLatency && quietStable {
		if p.quietSince.IsZero() {
			p.quietSince = now
		}
		if now.Sub(p.quietSince) >= p.cfg.QuietConfirm {
			p.state = StateQuiet
		} else {
			p.state = StateQuietCandidate
		}
		return p.state
	}

	p.quietSince = time.Time{}
	p.state = StateNormal
	return p.state
}

func (p *adaptivePolicy) updateBaseline(rate float64) {
	if !p.baselineReady {
		p.baselineRate = rate
		p.baselinePoints++
		if p.baselinePoints >= 4 {
			p.baselineReady = true
		}
		return
	}
	alpha := 0.002
	if rate > p.baselineRate {
		alpha = 0.20
	}
	p.baselineRate += alpha * (rate - p.baselineRate)
}

func (p *adaptivePolicy) updateSeasonal(now time.Time, rate float64) bool {
	const interval = 15 * time.Minute
	key := now.UnixNano() / int64(interval)
	index := seasonalIndex(now)
	window := &p.seasonalWindow
	if !window.active {
		*window = seasonalWindow{active: true, key: key, index: index, sum: rate, count: 1}
		return false
	}
	if key == window.key {
		window.sum += rate
		window.count++
		return false
	}

	// Commit one average per completed 15-minute interval. Scheduler polls are
	// deliberately not treated as independent seasonal observations; otherwise
	// a single quiet minute could masquerade as a learned weekly pattern.
	point := &p.seasonal[window.index]
	average := window.sum / float64(window.count)
	if point.samples == 0 {
		point.rate = average
	} else {
		// A slow EWMA learns recurring load shapes without treating one unusual
		// quiet or busy interval as the new schedule.
		point.rate += 0.05 * (average - point.rate)
	}
	point.samples++
	*window = seasonalWindow{active: true, key: key, index: index, sum: rate, count: 1}
	return true
}

func (p *adaptivePolicy) seasonalExpected(now time.Time) (float64, bool) {
	point := p.seasonal[seasonalIndex(now)]
	return point.rate, point.samples >= 2
}

func seasonalIndex(now time.Time) int {
	now = now.UTC()
	quarter := now.Hour()*4 + now.Minute()/15
	return int(now.Weekday())*24*4 + quarter
}

func (p *adaptivePolicy) snapshotModel() ModelSnapshot {
	if p == nil {
		return ModelSnapshot{}
	}
	snapshot := ModelSnapshot{
		Generation:     p.generation,
		BaselineRate:   p.baselineRate,
		BaselineReady:  p.baselineReady,
		BaselinePoints: uint32(p.baselinePoints),
	}
	for i, point := range p.seasonal {
		snapshot.Seasonal[i] = ModelPoint{Rate: point.rate, Samples: point.samples}
	}
	return snapshot
}

func (p *adaptivePolicy) restoreModel(snapshot ModelSnapshot) error {
	if p == nil {
		return ErrModelCorrupt
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	p.generation = snapshot.Generation
	p.baselineRate = snapshot.BaselineRate
	p.baselineReady = snapshot.BaselineReady
	p.baselinePoints = int(snapshot.BaselinePoints)
	for i, point := range snapshot.Seasonal {
		p.seasonal[i] = seasonalPoint{rate: point.Rate, samples: point.Samples}
	}
	// Transient load classification and incomplete windows never survive a
	// restart; beginning in StateNormal avoids admitting work from stale calm.
	p.state = StateNormal
	p.expectedRate = 0
	p.quietSince = time.Time{}
	p.pressureEnded = time.Time{}
	p.seasonalWindow = seasonalWindow{}
	return nil
}

func maxRatio(read, readTarget, write, writeTarget time.Duration) float64 {
	var ratio float64
	if read > 0 && readTarget > 0 {
		ratio = float64(read) / float64(readTarget)
	}
	if write > 0 && writeTarget > 0 {
		ratio = max(ratio, float64(write)/float64(writeTarget))
	}
	return ratio
}
