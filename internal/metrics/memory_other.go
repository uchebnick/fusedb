//go:build !darwin && !linux

package metrics

func physicalMemoryBytes() uint64 { return 0 }
