package segment

import (
	"fusedb/internal/compression"
	"fusedb/internal/disk"
)

// Segment starts mutable and becomes immutable after Freeze.
type Segment struct {
	Header Header
	Footer Footer
	Index  Index
	Bloom  BloomFilter

	frozen bool

	targetBlockSize int
	compression     CompressionKind
	dictionary      *compression.Dictionary
	fs              disk.FS
	file            disk.File
	path            string
	tempPath        string

	currentBlock     Block
	currentBlockSize int
	lastKey          []byte
	dataLength       uint64
}

// Frozen reports whether segment was finalized.
func (s *Segment) Frozen() bool {
	return s != nil && s.frozen
}

// Path returns final segment file path.
func (s Segment) Path() string {
	return s.path
}

// Len returns current segment file size in bytes.
func (s Segment) Len() int {
	name := s.path
	if !s.frozen && s.tempPath != "" {
		name = s.tempPath
	}
	if s.fs == nil || name == "" {
		return 0
	}
	info, err := s.fs.Stat(name)
	if err != nil {
		return 0
	}
	return int(info.Size())
}
