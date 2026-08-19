package disk

// IOObserver receives payload byte counts from a filesystem wrapper.
type IOObserver interface {
	AddDiskReadBytes(int)
	AddDiskWriteBytes(int)
}

// Meter wraps fs and records successful payload bytes. Metadata operations are
// deliberately not counted because the scheduler consumes a throughput signal,
// not an exact device-sector accounting value.
func Meter(fs FS, observer IOObserver) FS {
	if fs == nil || observer == nil {
		return fs
	}
	return &meteredFS{FS: fs, observer: observer}
}

type meteredFS struct {
	FS
	observer IOObserver
}

func (m *meteredFS) Create(name string) (File, error) {
	f, err := m.FS.Create(name)
	return m.file(f, err)
}

func (m *meteredFS) Open(name string) (File, error) {
	f, err := m.FS.Open(name)
	return m.file(f, err)
}

func (m *meteredFS) OpenReadWrite(name string) (File, error) {
	f, err := m.FS.OpenReadWrite(name)
	return m.file(f, err)
}

func (m *meteredFS) file(f File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	return &meteredFile{File: f, observer: m.observer}, nil
}

type meteredFile struct {
	File
	observer IOObserver
}

func (f *meteredFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.observer.AddDiskReadBytes(n)
	return n, err
}

func (f *meteredFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.observer.AddDiskReadBytes(n)
	return n, err
}

func (f *meteredFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.observer.AddDiskWriteBytes(n)
	return n, err
}

func (f *meteredFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.observer.AddDiskWriteBytes(n)
	return n, err
}
