package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/backup"
	"github.com/uchebnick/fusedb/internal/disk"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

func TestSchedulerModelSurvivesOpenBackupRestoreAndVerify(t *testing.T) {
	fs := disk.NewMemFS()
	now := time.Now().UTC()
	index := int(now.Weekday())*24*4 + now.Hour()*4 + now.Minute()/15
	want := scheduler.ModelSnapshot{
		Generation:     11,
		BaselineRate:   500,
		BaselineReady:  true,
		BaselinePoints: 4,
	}
	for i := range want.Seasonal {
		want.Seasonal[i] = scheduler.ModelPoint{Rate: 37.5, Samples: 3}
	}
	data, err := scheduler.EncodeModelSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteFileAtomically(fs, schedulerModelPath("scheduler-model"), data); err != nil {
		t.Fatal(err)
	}

	db, err := OpenDB(DBOptions{
		Dir:           "scheduler-model",
		FS:            fs,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := db.background.ModelSnapshot(); got.Generation != want.Generation || got.Seasonal[index] != want.Seasonal[index] {
		t.Fatalf("loaded model = %+v, want generation %d point %+v", got, want.Generation, want.Seasonal[index])
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if db.SchedulerLoad().ExpectedRequestRate == want.Seasonal[index].Rate {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := db.SchedulerLoad().ExpectedRequestRate; got != want.Seasonal[index].Rate {
		t.Fatalf("restored expected request rate = %v, want %v", got, want.Seasonal[index].Rate)
	}
	verified, err := db.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if verified.SchedulerModelGeneration != want.Generation {
		t.Fatalf("verified model generation = %d, want %d", verified.SchedulerModelGeneration, want.Generation)
	}
	backupReport, err := db.Backup(context.Background(), "scheduler-model.fbak")
	if err != nil {
		t.Fatal(err)
	}
	if backupReport.Files < 4 {
		t.Fatalf("backup omitted operational metadata: %+v", backupReport)
	}
	if _, err := backup.Restore(context.Background(), fs, "scheduler-model.fbak", "scheduler-model-restored"); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenDB(DBOptions{Dir: "scheduler-model-restored", FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	restoredReport, err := restored.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if restoredReport.SchedulerModelGeneration != want.Generation {
		t.Fatalf("restored model generation = %d, want %d", restoredReport.SchedulerModelGeneration, want.Generation)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptSchedulerModelIsObservableAndAutomaticallyHealed(t *testing.T) {
	fs := disk.NewMemFS()
	if err := disk.WriteFileAtomically(fs, schedulerModelPath("scheduler-model-corrupt"), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(DBOptions{
		Dir:        "scheduler-model-corrupt",
		FS:         fs,
		DisableWAL: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("optional corrupt scheduler state blocked open: %v", err)
	}
	defer db.CloseWithoutCheckpoint()
	if got := db.MetricsSnapshot().BackgroundFailed[enginemetrics.BackgroundSchedulerModelPersist]; got != 1 {
		t.Fatalf("scheduler model load failures = %d, want 1", got)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := loadSchedulerModel(fs, "scheduler-model-corrupt"); err == nil &&
			db.MetricsSnapshot().BackgroundCompleted[enginemetrics.BackgroundSchedulerModelPersist] > 0 {
			if _, err := db.Verify(context.Background()); err != nil {
				t.Fatalf("verify healed scheduler model: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("corrupt scheduler model was not healed in quiet window")
}

type schedulerModelSyncFaultFS struct {
	disk.FS
	mu            sync.Mutex
	failStateSync bool
	stateRenamed  bool
}

func (f *schedulerModelSyncFaultFS) Rename(oldName, newName string) error {
	if err := f.FS.Rename(oldName, newName); err != nil {
		return err
	}
	f.mu.Lock()
	if filepath.Base(newName) == DefaultSchedulerModelFileName {
		f.stateRenamed = true
	}
	f.mu.Unlock()
	return nil
}

func (f *schedulerModelSyncFaultFS) SyncDir(dir string) error {
	f.mu.Lock()
	fail := f.failStateSync && f.stateRenamed
	if fail {
		f.stateRenamed = false
	}
	f.mu.Unlock()
	if fail {
		return fmt.Errorf("injected scheduler model directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestSchedulerModelCloseFailureDoesNotImplyUserDataCommitUncertainty(t *testing.T) {
	base := disk.NewMemFS()
	fs := &schedulerModelSyncFaultFS{FS: base}
	db, err := OpenDB(DBOptions{Dir: "scheduler-model-close-fault", FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("durable-value"), 32)
	if err := db.Put([]byte("key"), want); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	fs.failStateSync = true
	fs.mu.Unlock()
	closeErr := db.Close()
	if !errors.Is(closeErr, ErrSchedulerModelPersistence) {
		t.Fatalf("close error = %v, want ErrSchedulerModelPersistence", closeErr)
	}
	if errors.Is(closeErr, disk.ErrCommitUncertain) {
		t.Fatalf("auxiliary model failure leaked user-data commit uncertainty: %v", closeErr)
	}
	fs.mu.Lock()
	fs.failStateSync = false
	fs.mu.Unlock()
	reopened, err := OpenDB(DBOptions{Dir: "scheduler-model-close-fault", FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, found, err := reopened.Get([]byte("key"))
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("user data after scheduler model failure = (%q,%v,%v)", got, found, err)
	}
}
