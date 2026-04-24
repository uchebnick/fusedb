package segment

import "testing"

func TestHeaderRoundTrip(t *testing.T) {
	header := Header{
		Version:      42,
		Compression:  CompressionZstdDict,
		DictionaryID: 17,
	}

	data, err := EncodeHeader(header)
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}

	decoded, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}

	if decoded.Version != header.Version {
		t.Fatalf("version = %d, want %d", decoded.Version, header.Version)
	}
	if decoded.Compression != header.Compression {
		t.Fatalf("compression = %d, want %d", decoded.Compression, header.Compression)
	}
	if decoded.DictionaryID != header.DictionaryID {
		t.Fatalf("dict id = %d, want %d", decoded.DictionaryID, header.DictionaryID)
	}
}

func TestHeaderValidateRawSegment(t *testing.T) {
	header := Header{
		Version:      1,
		Compression:  CompressionNone,
		DictionaryID: 0,
	}
	if err := header.Validate(); err != nil {
		t.Fatalf("validate raw header: %v", err)
	}
}

func TestHeaderRejectsUnexpectedDictionaryID(t *testing.T) {
	header := Header{
		Version:      7,
		Compression:  CompressionNone,
		DictionaryID: 5,
	}
	if err := header.Validate(); err == nil {
		t.Fatal("expected validation error for raw header with dict id")
	}
}
