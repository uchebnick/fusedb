//go:build darwin

package metrics

import "golang.org/x/sys/unix"

func physicalMemoryBytes() uint64 {
	memory, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return memory
}
