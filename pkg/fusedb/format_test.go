package fusedb

import "testing"

func TestFormatInfoExposesCurrentCompatibilityContract(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	info := db.Format()
	if info.Epoch != CurrentFormatEpoch || info.MinReaderEpoch != CurrentFormatEpoch ||
		info.MinWriterEpoch != CurrentFormatEpoch {
		t.Fatalf("format epochs = %+v", info)
	}
	wantRequired := FormatFeatureManifestV4 |
		FormatFeaturePerLeafWALWatermarks |
		FormatFeatureDictionaryGroups |
		FormatFeatureExternalDictionaryCatalog |
		FormatFeatureBoundedRecordSizes |
		FormatFeatureAtomicBatches
	if info.RequiredFeatures != wantRequired {
		t.Fatalf("required features = 0x%x, want 0x%x", info.RequiredFeatures, wantRequired)
	}
	if info.OptionalFeatures&FormatOptionalSchedulerModel == 0 {
		t.Fatalf("optional features = 0x%x, scheduler model bit missing", info.OptionalFeatures)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
