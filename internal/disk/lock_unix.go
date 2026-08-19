//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package disk

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func (osFS) Lock(name string) (Lock, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &osLock{file: file}, nil
}

type osLock struct {
	file *os.File
}

func (l *osLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}
