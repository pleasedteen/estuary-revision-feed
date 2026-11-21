// Package memstats tracks the process heap high-water marks used to verify
// that synchronization and restart recovery stay within their heap budget.
//
// The sampler reads the Go runtime's heap-objects metric on a short tick and
// keeps two peaks: one since process start (used to measure restart
// recovery, which happens before the HTTP surface is up) and one since the
// last Reset (used to measure a sync run against a steady-state baseline).
package memstats

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

// Sampler samples the live heap in the background.
type Sampler struct {
	mu     sync.Mutex
	peak   uint64 // high-water mark since Start
	rpeak  uint64 // high-water mark since the last Reset
	done   chan struct{}
	closed sync.Once
}

// Start launches a sampler ticking every interval.
func Start(interval time.Duration) *Sampler {
	s := &Sampler{done: make(chan struct{})}
	s.observe()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-t.C:
				s.observe()
			}
		}
	}()
	return s
}

func heapAlloc() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

func (s *Sampler) observe() {
	v := heapAlloc()
	s.mu.Lock()
	if v > s.peak {
		s.peak = v
	}
	if v > s.rpeak {
		s.rpeak = v
	}
	s.mu.Unlock()
}

// Current returns the live heap right now.
func (s *Sampler) Current() uint64 { return heapAlloc() }

// Peak returns the highest sampled heap since Start.
func (s *Sampler) Peak() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// PeakSinceReset returns the highest sampled heap since the last Reset.
func (s *Sampler) PeakSinceReset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rpeak
}

// Reset forces garbage collection to reach a steady-state baseline, then
// restarts the resettable high-water mark from that baseline. It returns the
// baseline heap in bytes.
func (s *Sampler) Reset() uint64 {
	runtime.GC()
	runtime.GC()
	v := heapAlloc()
	s.mu.Lock()
	s.rpeak = v
	s.mu.Unlock()
	return v
}

// Close stops the sampling goroutine.
func (s *Sampler) Close() {
	s.closed.Do(func() { close(s.done) })
}
