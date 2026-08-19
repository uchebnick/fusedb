//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package disk

func (osFS) Lock(string) (Lock, error) {
	return nil, ErrLockUnsupported
}
