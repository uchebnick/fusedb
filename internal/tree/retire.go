package tree

import (
	"time"

	"github.com/uchebnick/fusedb/internal/segment"
)

const (
	// readerRetireTTL is how long a replaced segment stays readable after it
	// was superseded. Lookups take no lock, so one may already be inside a
	// segment when a merge replaces it; deleting the file immediately would
	// turn that lookup into an error.
	readerRetireTTL = 2 * time.Second

	readerCleanupTick    = 100 * time.Millisecond
	retiredReaderBufSize = 256
)

type retiredReader struct {
	retiredAt time.Time
	reader    *segment.Reader
}

// startRetireWorker runs the single cleanup goroutine of the tree.
//
// Leaves get this as their retire hook instead of keeping their own goroutine:
// the work is the same for every leaf and a tree may hold thousands of them.
func (t *Tree) startRetireWorker() {
	go func() {
		defer close(t.retireDone)

		ticker := time.NewTicker(readerCleanupTick)
		defer ticker.Stop()

		queue := make([]retiredReader, 0, 16)
		for {
			select {
			case <-t.retireStop:
				for _, retired := range queue {
					closeRetiredReader(retired)
				}
				for {
					select {
					case retired := <-t.retired:
						closeRetiredReader(retired)
					default:
						return
					}
				}
			case retired := <-t.retired:
				queue = append(queue, retired)
			case now := <-ticker.C:
				queue = closeExpiredReaders(queue, now)
			}
		}
	}()
}

func (t *Tree) stopRetireWorker() {
	close(t.retireStop)
	<-t.retireDone
}

func (t *Tree) retireReader(reader *segment.Reader) {
	if reader == nil {
		return
	}
	retired := retiredReader{retiredAt: time.Now(), reader: reader}
	select {
	case t.retired <- retired:
	case <-t.retireStop:
		// The worker is gone, so nobody would ever drain this reader.
		closeRetiredReader(retired)
	}
}

func closeExpiredReaders(queue []retiredReader, now time.Time) []retiredReader {
	keep := queue[:0]
	for _, retired := range queue {
		if now.Sub(retired.retiredAt) >= readerRetireTTL {
			closeRetiredReader(retired)
			continue
		}
		keep = append(keep, retired)
	}
	return keep
}

func closeRetiredReader(retired retiredReader) {
	if retired.reader == nil {
		return
	}
	replaced := retired.reader.Segment()
	_ = retired.reader.Close()
	if replaced != nil {
		_ = replaced.Remove()
	}
}
