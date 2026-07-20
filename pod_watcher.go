package main

import (
	"context"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// watchPods watches pods labelled app=api or app=spa and records
// init container durations and pod ready time. No secret access required —
// all timing data is on the pod spec/status itself.
func (w *watcher) watchPods(ctx context.Context) {
	retryWatch(ctx, "pods", func() error { return w.doWatchPods(ctx) })
}

func (w *watcher) doWatchPods(ctx context.Context) error {
	podGVR := podGVR()
	// Only Api and Spa pods use init containers for service-binding; other XR
	// kinds provision eagerly and don't follow the same startup pattern.
	wi, err := w.client.Resource(podGVR).Namespace("").Watch(ctx, metav1.ListOptions{
		LabelSelector: "app in (api,spa)",
	})
	if err != nil {
		return err
	}
	defer wi.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return nil
			}
			if ev.Type != watch.Added && ev.Type != watch.Modified {
				continue
			}
			obj, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			w.handlePod(obj)
		}
	}
}

func (w *watcher) handlePod(obj *unstructured.Unstructured) {
	name := obj.GetName()
	ns := obj.GetNamespace()
	podCreated := obj.GetCreationTimestamp().Time

	w.recordInitContainers(obj, name, ns, podCreated)
	w.recordPodReady(obj, name, ns, podCreated)
}

func (w *watcher) recordInitContainers(obj *unstructured.Unstructured, podName, ns string, podCreated time.Time) {
	statuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "initContainerStatuses")
	for _, s := range statuses {
		sm, ok := s.(map[string]interface{})
		if !ok {
			continue
		}
		containerName, _ := sm["name"].(string)
		if containerName == "" {
			continue
		}
		key := ns + "/" + podName + "/" + containerName

		// Init containers re-run after a node or container runtime restart. The
		// terminated state then describes the re-run (typically instant), so
		// finishedAt - podCreated would report the pod's age, not the binding wait.
		// The first-run timing is gone from the status at that point — skip.
		restartCount, _, _ := unstructured.NestedInt64(sm, "restartCount")
		if restartCount > 0 {
			continue
		}

		terminated, _, _ := unstructured.NestedMap(sm, "state", "terminated")
		if terminated == nil {
			continue
		}
		finishedAt, _ := terminated["finishedAt"].(string)
		if finishedAt == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, finishedAt)
		if err != nil {
			continue
		}

		w.mu.Lock()
		if !w.initContainerRecorded[key] {
			// initContainerRecorded is intentionally never cleared: only the first
			// run is a valid observation, and re-runs are filtered out above.
			w.initContainerRecorded[key] = true
			elapsed := t.Sub(podCreated).Seconds()
			podInitContainerDuration.WithLabelValues(containerName, ns, podName).Set(elapsed)
			if podCreated.After(w.startedAt) {
				initContainerSeconds.WithLabelValues(containerName, ns).Observe(elapsed)
				slog.Info("init container done", "container", containerName, "namespace", ns, "pod", podName, "seconds", elapsed)
			}
		}
		w.mu.Unlock()
	}
}

func (w *watcher) recordPodReady(obj *unstructured.Unstructured, podName, ns string, podCreated time.Time) {
	key := ns + "/" + podName

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cm["type"] != "Ready" || cm["status"] != "True" {
			continue
		}
		t, _ := cm["lastTransitionTime"].(string)
		readyAt, err := time.Parse(time.RFC3339, t)
		if err != nil {
			continue
		}

		w.mu.Lock()
		if !w.podReadyRecorded[key] {
			w.podReadyRecorded[key] = true
			if podCreated.After(w.startedAt) {
				elapsed := readyAt.Sub(podCreated).Seconds()
				podReadyDuration.WithLabelValues(ns, podName).Set(elapsed)
				podTimeToReady.WithLabelValues(ns).Observe(elapsed)
				slog.Info("pod ready", "pod", podName, "namespace", ns, "seconds", elapsed)
			}
		}
		w.mu.Unlock()
		return
	}
}
