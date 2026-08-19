package prometheusrules

import (
	"bytes"
	"strings"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"gopkg.in/yaml.v3"
)

type ruleFile struct {
	Groups []ruleGroup `yaml:"groups"`
}

type ruleGroup struct {
	Name     string `yaml:"name"`
	Interval string `yaml:"interval"`
	Rules    []rule `yaml:"rules"`
}

type rule struct {
	Alert       string            `yaml:"alert"`
	Record      string            `yaml:"record"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

func TestRulesAreStructurallyComplete(t *testing.T) {
	data, err := disk.ReadFileLimited(disk.DefaultFS, "fusedb.rules.yml", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var file ruleFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		t.Fatalf("decode rules: %v", err)
	}
	if len(file.Groups) != 2 {
		t.Fatalf("groups = %d, want recording and alert groups", len(file.Groups))
	}

	names := make(map[string]struct{})
	alerts := 0
	records := 0
	allExpressions := ""
	for _, group := range file.Groups {
		if group.Name == "" || group.Interval == "" || len(group.Rules) == 0 {
			t.Fatalf("incomplete group: %+v", group)
		}
		for _, rule := range group.Rules {
			if (rule.Alert == "") == (rule.Record == "") {
				t.Fatalf("rule must set exactly one of alert/record: %+v", rule)
			}
			name := rule.Alert + rule.Record
			if _, duplicate := names[name]; duplicate {
				t.Fatalf("duplicate rule %q", name)
			}
			names[name] = struct{}{}
			if strings.TrimSpace(rule.Expr) == "" {
				t.Fatalf("rule %q has no expression", name)
			}
			allExpressions += "\n" + rule.Expr
			if rule.Alert != "" {
				alerts++
				if rule.Labels["severity"] == "" || rule.Annotations["summary"] == "" || rule.Annotations["description"] == "" {
					t.Fatalf("alert %q lacks routing or operator context", name)
				}
			} else {
				records++
			}
		}
	}
	if alerts < 8 || records != 3 {
		t.Fatalf("rules = %d alerts, %d records", alerts, records)
	}
	for _, metric := range []string{
		"fusedb_ready", "fusedb_terminal_error", "fusedb_commit_uncertain_errors_total",
		"fusedb_background_jobs_total", "fusedb_scheduler_state",
		"fusedb_operation_duration_seconds_bucket", "fusedb_pending_merge_leaves",
		"fusedb_wal_checkpoint_debt_bytes", "fusedb_resource_utilization_ratio",
	} {
		if !strings.Contains(allExpressions, metric) {
			t.Errorf("rules do not cover %s", metric)
		}
	}
}
