// Package limits defines hard safety ceilings for persisted FuseDB data.
//
// These are format-level availability limits, not scheduler budgets. Every
// decoder must reject data above them before allocating from a length or file
// size supplied by storage. Keeping the ceilings independent of runtime
// options guarantees that a database remains readable after configuration
// changes and that a corrupt sparse file cannot request unbounded memory.
package limits

const (
	// MaxKeyBytes bounds every key accepted into the WAL or a segment.
	MaxKeyBytes = 1 << 20 // 1 MiB
	// MaxValueBytes bounds a user byte value. Counter encodings are smaller.
	MaxValueBytes = 64 << 20 // 64 MiB

	// WAL stores raw Put payloads; segments add one internal value-kind byte.
	MaxWALPayloadBytes   = MaxValueBytes
	MaxEncodedValueBytes = MaxValueBytes + 1
	// MaxBatchMutations bounds decoder allocation and the time one atomic
	// foreground transaction may exclude other operations.
	MaxBatchMutations = 4 << 10
	// MaxEncodedBlockBytes allows one maximum-size key/value entry plus block
	// framing. Normal blocks remain close to 4 KiB.
	MaxEncodedBlockBytes = MaxKeyBytes + MaxEncodedValueBytes + (1 << 20)
	// MaxSegmentKeys bounds bloom sizing and manifest key-count metadata.
	MaxSegmentKeys = 16 << 20

	// Metadata limits are deliberately much larger than normal files. A 64 MiB
	// manifest holds well over a million ordinary leaf records.
	MaxManifestBytes        = 64 << 20
	MaxGroupCatalogBytes    = 32 << 20
	MaxSegmentMetadataBytes = 64 << 20
	MaxFormatBytes          = 4 << 10
	MaxSchedulerModelBytes  = 16 << 10
	MaxDictionaryBytes      = 64 << 10 // LZ4's effective dictionary window
)
