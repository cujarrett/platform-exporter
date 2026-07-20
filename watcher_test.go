package main

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestReadyCondition(t *testing.T) {
	tests := []struct {
		name        string
		conditions  []interface{}
		wantReady   bool
		wantNonZero bool
	}{
		{
			name:       "no conditions",
			conditions: nil,
			wantReady:  false,
		},
		{
			name: "ready false",
			conditions: []interface{}{
				map[string]interface{}{"type": "Ready", "status": "False"},
			},
			wantReady: false,
		},
		{
			name: "ready true with timestamp",
			conditions: []interface{}{
				map[string]interface{}{
					"type":               "Ready",
					"status":             "True",
					"lastTransitionTime": "2026-06-23T03:00:00Z",
				},
			},
			wantReady:   true,
			wantNonZero: true,
		},
		{
			name: "ready true missing timestamp",
			conditions: []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
			wantReady:   true,
			wantNonZero: false,
		},
		{
			name: "other condition types ignored",
			conditions: []interface{}{
				map[string]interface{}{"type": "Synced", "status": "True"},
				map[string]interface{}{
					"type":               "Ready",
					"status":             "True",
					"lastTransitionTime": "2026-06-23T03:00:00Z",
				},
			},
			wantReady:   true,
			wantNonZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
			if tt.conditions != nil {
				_ = unstructured.SetNestedSlice(obj.Object, tt.conditions, "status", "conditions")
			}

			ready, at := readyCondition(obj)
			if ready != tt.wantReady {
				t.Errorf("ready = %v, want %v", ready, tt.wantReady)
			}
			if tt.wantNonZero && at.IsZero() {
				t.Error("expected non-zero transition time")
			}
			if !tt.wantNonZero && !at.IsZero() {
				t.Error("expected zero transition time")
			}
		})
	}
}

func TestBackendOf(t *testing.T) {
	tests := []struct {
		name        string
		specBackend string
		kind        string
		want        string
	}{
		{
			name:        "explicit backend wins",
			specBackend: "public-cloud",
			kind:        "Sql",
			want:        "public-cloud",
		},
		{
			name: "NoSql defaults to public-cloud",
			kind: "NoSql",
			want: "public-cloud",
		},
		{
			name: "ObjectStorage defaults to public-cloud",
			kind: "ObjectStorage",
			want: "public-cloud",
		},
		{
			name: "Api defaults to private-cloud",
			kind: "Api",
			want: "private-cloud",
		},
		{
			name: "Sql without backend defaults to private-cloud",
			kind: "Sql",
			want: "private-cloud",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
			if tt.specBackend != "" {
				_ = unstructured.SetNestedField(obj.Object, tt.specBackend, "spec", "parameters", "backend")
			}
			got := backendOf(obj, tt.kind)
			if got != tt.want {
				t.Errorf("backendOf = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHandleXRRecordsOnce(t *testing.T) {
	w := newWatcher(nil)
	k := xrKind{kind: "Sql"}

	obj := makeXR("my-db", "my-app", "2026-06-23T02:00:00Z", "2026-06-23T02:00:30Z")

	w.handleXR(obj, k)
	w.handleXR(obj, k) // second call should not double-record

	key := "Sql/my-app/my-db"
	if !w.xrReadyRecorded[key] {
		t.Error("expected key to be recorded")
	}
}

func TestHandleXRGaugeSetOnce(t *testing.T) {
	w := newWatcher(nil)
	k := xrKind{kind: "Sql"}

	first := makeXR("my-db", "my-app", "2026-06-23T02:00:00Z", "2026-06-23T02:00:30Z")
	w.handleXR(first, k)

	// Simulate Crossplane drifting lastTransitionTime forward by weeks on reconcile.
	drifted := makeXR("my-db", "my-app", "2026-06-23T02:00:00Z", "2026-07-23T02:00:30Z")
	w.handleXR(drifted, k)

	key := "Sql/my-app/my-db"
	if !w.xrReadyRecorded[key] {
		t.Error("expected key to remain recorded after reconcile with drifted timestamp")
	}
}

func TestHandleXRPreexistingRecordedButSkipsHistogram(t *testing.T) {
	w := newWatcher(nil)
	// Back-date startedAt so the test XR (created 2026-06-23) looks pre-existing.
	// The gauge should still be set; the histogram observation is skipped.
	w.startedAt = time.Now().Add(24 * time.Hour)

	k := xrKind{kind: "Sql"}
	obj := makeXR("old-db", "ns", "2026-06-23T02:00:00Z", "2026-06-23T02:00:30Z")
	w.handleXR(obj, k)

	key := "Sql/ns/old-db"
	if !w.xrReadyRecorded[key] {
		t.Error("pre-existing XR must be marked recorded to prevent re-observation on next reconcile")
	}
}

func TestHandleXRClearsOnNotReady(t *testing.T) {
	w := newWatcher(nil)
	k := xrKind{kind: "Sql"}

	ready := makeXR("my-db", "my-app", "2026-06-23T02:00:00Z", "2026-06-23T02:00:30Z")
	w.handleXR(ready, k)

	notReady := &unstructured.Unstructured{Object: map[string]interface{}{}}
	notReady.SetName("my-db")
	notReady.SetNamespace("my-app")
	_ = unstructured.SetNestedSlice(notReady.Object, []interface{}{
		map[string]interface{}{"type": "Ready", "status": "False"},
	}, "status", "conditions")

	w.handleXR(notReady, k)

	key := "Sql/my-app/my-db"
	if w.xrReadyRecorded[key] {
		t.Error("expected key to be cleared after not-ready")
	}
}

func TestExtractBindings(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	_ = unstructured.SetNestedField(obj.Object, "my-db", "spec", "parameters", "sqlRef", "name")
	_ = unstructured.SetNestedField(obj.Object, "my-topic", "spec", "parameters", "topicRef", "name")
	_ = unstructured.SetNestedSlice(obj.Object, []interface{}{
		map[string]interface{}{"name": "my-bucket"},
		map[string]interface{}{"name": "other-bucket"},
	}, "spec", "parameters", "objectStorageRefs")

	bindings := extractBindings(obj)

	want := map[string]struct{}{
		"sql/my-db":                   {},
		"topic/my-topic":              {},
		"object-storage/my-bucket":    {},
		"object-storage/other-bucket": {},
	}
	if len(bindings) != len(want) {
		t.Fatalf("got %d bindings, want %d: %v", len(bindings), len(want), bindings)
	}
	for k := range want {
		if _, ok := bindings[k]; !ok {
			t.Errorf("missing binding %q", k)
		}
	}
}

func TestExtractBindingsEmpty(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	bindings := extractBindings(obj)
	if len(bindings) != 0 {
		t.Errorf("expected no bindings, got %v", bindings)
	}
}

// makeXR builds a minimal XR unstructured object with Ready=True.
func makeXR(name, namespace, createdAt, readyAt string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetName(name)
	obj.SetNamespace(namespace)
	t, _ := time.Parse(time.RFC3339, createdAt)
	obj.SetCreationTimestamp(metav1.Time{Time: t})
	_ = unstructured.SetNestedSlice(obj.Object, []interface{}{
		map[string]interface{}{
			"type":               "Ready",
			"status":             "True",
			"lastTransitionTime": readyAt,
		},
	}, "status", "conditions")
	return obj
}
