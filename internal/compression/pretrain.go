package compression

// PretrainOptions configures cold-start dictionary training.
type PretrainOptions struct {
	ID         uint32
	Size       int
	Level      int
	Samples    [][]byte
	CompatV155 bool
}

// PretrainDictionary trains and opens a reusable LZ4 dictionary.
//
// Callers should pass samples that match the real compression unit. For segment
// compression this means encoded raw block bytes, not standalone values.
func PretrainDictionary(opts PretrainOptions) (*Dictionary, error) {
	raw, err := TrainDictionary(TrainOptions(opts))
	if err != nil {
		return nil, err
	}
	return NewDictionaryLevel(opts.ID, raw, opts.Level)
}
