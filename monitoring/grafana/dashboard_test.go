package grafana

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

type dashboard struct {
	Title  string  `json:"title"`
	UID    string  `json:"uid"`
	Panels []panel `json:"panels"`
}

type panel struct {
	ID      int      `json:"id"`
	Title   string   `json:"title"`
	Type    string   `json:"type"`
	Targets []target `json:"targets"`
}

type target struct {
	Expr string `json:"expr"`
}

func TestOverviewDashboardIsImportableAndCoversOperations(t *testing.T) {
	data, err := disk.ReadFileLimited(disk.DefaultFS, "fusedb-overview.json", 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	var value dashboard
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode dashboard: %v", err)
	}
	if value.Title != "FuseDB / Overview" || value.UID != "fusedb-overview" {
		t.Fatalf("dashboard identity = %q/%q", value.Title, value.UID)
	}
	if len(value.Panels) < 10 {
		t.Fatalf("panels = %d, want at least 10", len(value.Panels))
	}
	ids := make(map[int]struct{}, len(value.Panels))
	queries := ""
	for _, panel := range value.Panels {
		if panel.ID <= 0 || panel.Title == "" || panel.Type == "" || len(panel.Targets) == 0 {
			t.Fatalf("incomplete panel: %+v", panel)
		}
		if _, duplicate := ids[panel.ID]; duplicate {
			t.Fatalf("duplicate panel id %d", panel.ID)
		}
		ids[panel.ID] = struct{}{}
		for _, target := range panel.Targets {
			if strings.TrimSpace(target.Expr) == "" {
				t.Fatalf("panel %q has empty query", panel.Title)
			}
			queries += "\n" + target.Expr
		}
	}
	for _, signal := range []string{
		"fusedb_ready", "fusedb_terminal_errors_total",
		"fusedb:operation_p95_seconds:rate5m", "fusedb:operation_p99_seconds:rate5m",
		"fusedb:operation_rate:rate5m", "fusedb_pending_merge_leaves",
		"fusedb_buffered_bytes", "fusedb_wal_checkpoint_debt_bytes",
		"fusedb_scheduler_state", "fusedb_resource_utilization_ratio",
		"fusedb_background_jobs_total", "fusedb_disk_io_bytes_total",
	} {
		if !strings.Contains(queries, signal) {
			t.Errorf("dashboard does not cover %s", signal)
		}
	}
}
