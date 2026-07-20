package main

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestRecordInitContainersRecordsOnce(t *testing.T) {
	w := newWatcher(nil)
	pod := makePod("api-foo-abc", "my-app", "2026-06-23T03:00:00Z", []initContainer{
		{name: "wait-for-sql-binding", finishedAt: "2026-06-23T03:00:20Z"},
		{name: "wait-for-cache-binding", finishedAt: "2026-06-23T03:00:25Z"},
	})

	podCreated, _ := time.Parse(time.RFC3339, "2026-06-23T03:00:00Z")
	w.recordInitContainers(pod, "api-foo-abc", "my-app", podCreated)
	w.recordInitContainers(pod, "api-foo-abc", "my-app", podCreated) // should not double-record

	sqlKey := "my-app/api-foo-abc/wait-for-sql-binding"
	cacheKey := "my-app/api-foo-abc/wait-for-cache-binding"
	if !w.initContainerRecorded[sqlKey] {
		t.Errorf("expected %q to be recorded", sqlKey)
	}
	if !w.initContainerRecorded[cacheKey] {
		t.Errorf("expected %q to be recorded", cacheKey)
	}
}

func TestRecordInitContainersSkipsRunning(t *testing.T) {
	w := newWatcher(nil)

	pod := &unstructured.Unstructured{Object: map[string]interface{}{}}
	_ = unstructured.SetNestedSlice(pod.Object, []interface{}{
		map[string]interface{}{
			"name":  "wait-for-sql-binding",
			"state": map[string]interface{}{"running": map[string]interface{}{}},
		},
	}, "status", "initContainerStatuses")

	podCreated := time.Now()
	w.recordInitContainers(pod, "api-foo", "my-app", podCreated)

	if len(w.initContainerRecorded) != 0 {
		t.Error("expected no entries recorded for running init container")
	}
}

func TestRecordInitContainersSkipsRestarted(t *testing.T) {
	w := newWatcher(nil)

	// After a node reboot the init container re-runs; the terminated state then
	// describes the re-run, so finishedAt - podCreated would be the pod's age.
	pod := &unstructured.Unstructured{Object: map[string]interface{}{}}
	_ = unstructured.SetNestedSlice(pod.Object, []interface{}{
		map[string]interface{}{
			"name":         "wait-for-cache-binding",
			"restartCount": int64(1),
			"state": map[string]interface{}{
				"terminated": map[string]interface{}{
					"finishedAt": "2026-06-24T03:00:00Z",
					"exitCode":   int64(0),
				},
			},
		},
	}, "status", "initContainerStatuses")

	podCreated, _ := time.Parse(time.RFC3339, "2026-06-23T03:00:00Z")
	w.recordInitContainers(pod, "api-foo", "my-app", podCreated)

	if len(w.initContainerRecorded) != 0 {
		t.Error("expected no entries recorded for restarted init container")
	}
}

func TestRecordPodReadyRecordsOnce(t *testing.T) {
	w := newWatcher(nil)
	pod := makePod("api-foo-abc", "my-app", "2026-06-23T03:00:00Z", nil)
	_ = unstructured.SetNestedSlice(pod.Object, []interface{}{
		map[string]interface{}{
			"type":               "Ready",
			"status":             "True",
			"lastTransitionTime": "2026-06-23T03:01:00Z",
		},
	}, "status", "conditions")

	podCreated, _ := time.Parse(time.RFC3339, "2026-06-23T03:00:00Z")
	w.recordPodReady(pod, "api-foo-abc", "my-app", podCreated)
	w.recordPodReady(pod, "api-foo-abc", "my-app", podCreated) // should not double-record

	key := "my-app/api-foo-abc"
	if !w.podReadyRecorded[key] {
		t.Errorf("expected %q to be recorded", key)
	}
}

func TestRecordPodReadySkipsNotReady(t *testing.T) {
	w := newWatcher(nil)
	pod := &unstructured.Unstructured{Object: map[string]interface{}{}}
	_ = unstructured.SetNestedSlice(pod.Object, []interface{}{
		map[string]interface{}{"type": "Ready", "status": "False"},
	}, "status", "conditions")

	w.recordPodReady(pod, "api-foo", "my-app", time.Now())

	if len(w.podReadyRecorded) != 0 {
		t.Error("expected no entries recorded for not-ready pod")
	}
}

// helpers

type initContainer struct {
	name       string
	finishedAt string
}

func makePod(name, namespace, createdAt string, initContainers []initContainer) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetName(name)
	obj.SetNamespace(namespace)
	t, _ := time.Parse(time.RFC3339, createdAt)
	obj.SetCreationTimestamp(metav1Time(t))

	statuses := make([]interface{}, 0, len(initContainers))
	for _, ic := range initContainers {
		statuses = append(statuses, map[string]interface{}{
			"name": ic.name,
			"state": map[string]interface{}{
				"terminated": map[string]interface{}{
					"finishedAt": ic.finishedAt,
					"exitCode":   int64(0),
				},
			},
		})
	}
	if len(statuses) > 0 {
		_ = unstructured.SetNestedSlice(obj.Object, statuses, "status", "initContainerStatuses")
	}
	return obj
}

func metav1Time(t time.Time) metav1.Time {
	return metav1.Time{Time: t}
}
