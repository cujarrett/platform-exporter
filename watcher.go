package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type xrKind struct {
	group    string
	version  string
	resource string
	kind     string
}

type managedKind struct {
	group    string
	version  string
	resource string
	kind     string
}

var watchedXRs = []xrKind{
	{"platform.local.lab", "v1alpha1", "apis", "Api"},
	{"platform.local.lab", "v1alpha1", "spas", "Spa"},
	{"platform.local.lab", "v1alpha1", "sqls", "Sql"},
	{"platform.local.lab", "v1alpha1", "nosqls", "NoSql"},
	{"platform.local.lab", "v1alpha1", "objectstorages", "ObjectStorage"},
	{"platform.local.lab", "v1alpha1", "caches", "Cache"},
	{"platform.local.lab", "v1alpha1", "topics", "Topic"},
	{"platform.local.lab", "v1alpha1", "subscriptions", "Subscription"},
}

var watchedManaged = []managedKind{
	{"dynamodb.aws.upbound.io", "v1beta1", "tables", "DynamoDBTable"},
	{"elasticache.aws.upbound.io", "v1beta2", "replicationgroups", "ElastiCacheReplicationGroup"},
	{"elasticache.aws.upbound.io", "v1beta1", "users", "ElastiCacheUser"},
	{"elasticache.aws.upbound.io", "v1beta1", "usergroups", "ElastiCacheUserGroup"},
	{"iam.aws.upbound.io", "v1beta1", "roles", "IAMRole"},
	{"jetstream.nats.io", "v1beta2", "streams", "NATSStream"},
	{"jetstream.nats.io", "v1beta2", "consumers", "NATSConsumer"},
	{"rds.aws.upbound.io", "v1beta3", "instances", "RDSInstance"},
	{"rolesanywhere.aws.upbound.io", "v1beta1", "profiles", "RolesAnywhereProfile"},
	{"s3.aws.upbound.io", "v1beta2", "buckets", "S3Bucket"},
}

// xrIdentity is the label set behind a live XR's gauges, kept around so a
// deletion (explicit or discovered via reconcile) can clear them without the
// unstructured object in hand.
type xrIdentity struct {
	kind, name, ns, backend string
}

// managedIdentity is the managed-resource equivalent of xrIdentity.
type managedIdentity struct {
	kind, name, ns string
}

type watcher struct {
	client    dynamic.Interface
	startedAt time.Time

	mu                    sync.Mutex
	xrReadyRecorded       map[string]bool                // kind/namespace/name
	managedReadyRecorded  map[string]bool                // kind/name
	initContainerRecorded map[string]bool                // namespace/pod/container
	podReadyRecorded      map[string]bool                // namespace/pod
	xrBindings            map[string]map[string]struct{} // consumer name → active "type/provider" keys
	xrLive                map[string]xrIdentity          // kind/namespace/name → labels, for reconcile-on-reconnect
	managedLive           map[string]managedIdentity     // kind/name → labels, for reconcile-on-reconnect
}

func newWatcher(client dynamic.Interface) *watcher {
	return &watcher{
		client:                client,
		startedAt:             time.Now(),
		xrReadyRecorded:       make(map[string]bool),
		managedReadyRecorded:  make(map[string]bool),
		initContainerRecorded: make(map[string]bool),
		podReadyRecorded:      make(map[string]bool),
		xrBindings:            make(map[string]map[string]struct{}),
		xrLive:                make(map[string]xrIdentity),
		managedLive:           make(map[string]managedIdentity),
	}
}

func (w *watcher) run(ctx context.Context) {
	for _, k := range watchedXRs {
		go w.watchXR(ctx, k)
	}
	for _, k := range watchedManaged {
		go w.watchManaged(ctx, k)
	}
	go w.watchPods(ctx)
	<-ctx.Done()
}

// ── XR watching ──────────────────────────────────────────────────────────────

func (w *watcher) watchXR(ctx context.Context, k xrKind) {
	gvr := schema.GroupVersionResource{Group: k.group, Version: k.version, Resource: k.resource}
	retryWatch(ctx, k.kind, func() error { return w.doWatchXR(ctx, gvr, k) })
}

func (w *watcher) doWatchXR(ctx context.Context, gvr schema.GroupVersionResource, k xrKind) error {
	// List first and reconcile against what we last knew was live: the watch is
	// re-opened every few minutes (see retryWatch), and a delete that lands in
	// the gap between the old watch closing and this list running would
	// otherwise never fire a watch.Deleted event, leaking its gauges forever.
	list, err := w.client.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	w.reconcileXR(list.Items, k)

	// Namespace("") is required for cluster-scoped resources; for namespaced resources it means all namespaces.
	// Watching from the list's resourceVersion avoids missing events created between the list and the watch starting.
	wi, err := w.client.Resource(gvr).Namespace("").Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
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
			if ev.Type == watch.Deleted {
				if obj, ok := ev.Object.(*unstructured.Unstructured); ok {
					w.deleteXR(k.kind + "/" + obj.GetNamespace() + "/" + obj.GetName())
				}
				continue
			}
			if ev.Type != watch.Added && ev.Type != watch.Modified {
				continue
			}
			obj, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			w.handleXR(obj, k)
		}
	}
}

// reconcileXR refreshes gauges for every currently-live XR of kind k and
// deletes gauges for any XR this watcher previously tracked (of the same
// kind) that is no longer in the list — the case a missed watch.Deleted
// event would otherwise leak.
func (w *watcher) reconcileXR(items []unstructured.Unstructured, k xrKind) {
	seen := make(map[string]struct{}, len(items))
	for i := range items {
		obj := &items[i]
		seen[k.kind+"/"+obj.GetNamespace()+"/"+obj.GetName()] = struct{}{}
		w.handleXR(obj, k)
	}

	w.mu.Lock()
	var stale []string
	for key, id := range w.xrLive {
		if id.kind != k.kind {
			continue
		}
		if _, ok := seen[key]; !ok {
			stale = append(stale, key)
		}
	}
	w.mu.Unlock()

	for _, key := range stale {
		w.deleteXR(key)
	}
}

// deleteXR clears every gauge tracked for a given XR key ("kind/namespace/name").
func (w *watcher) deleteXR(key string) {
	w.mu.Lock()
	id, ok := w.xrLive[key]
	if ok {
		delete(w.xrLive, key)
		delete(w.xrReadyRecorded, key)
	}
	w.mu.Unlock()
	if !ok {
		return
	}
	w.clearBindings(id.name, id.kind)
	xrReady.DeleteLabelValues(id.kind, id.name, id.ns, id.backend)
	xrReadyDuration.DeleteLabelValues(id.kind, id.name, id.ns, id.backend)
}

func (w *watcher) handleXR(obj *unstructured.Unstructured, k xrKind) {
	name := obj.GetName()
	ns := obj.GetNamespace()
	created := obj.GetCreationTimestamp().Time
	backend := backendOf(obj, k.kind)
	key := k.kind + "/" + ns + "/" + name

	ready, readyAt := readyCondition(obj)
	gauge := 0.0
	if ready {
		gauge = 1.0
	}
	xrReady.WithLabelValues(k.kind, name, ns, backend).Set(gauge)

	if k.kind == "Api" {
		w.syncBindings(name, k.kind, extractBindings(obj))
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.xrLive[key] = xrIdentity{kind: k.kind, name: name, ns: ns, backend: backend}

	if ready && !readyAt.IsZero() {
		if !w.xrReadyRecorded[key] {
			w.xrReadyRecorded[key] = true
			// Pre-existing XRs seen on the first reconcile event after a restart have
			// unreliable lastTransitionTime values (Crossplane drifts them on re-reconciles),
			// so their elapsed would be current age rather than actual provisioning time.
			// Skip both the gauge and histogram for those; only record XRs provisioned
			// while this process has been running.
			if created.After(w.startedAt) {
				elapsed := readyAt.Sub(created).Seconds()
				xrReadyDuration.WithLabelValues(k.kind, name, ns, backend).Set(elapsed)
				xrTimeToReady.WithLabelValues(k.kind, backend).Observe(elapsed)
				slog.Info("xr ready", "kind", k.kind, "name", name, "namespace", ns, "backend", backend, "seconds", elapsed)
			}
		}
	}
	if !ready {
		// Clear so a ready→not-ready→ready flip records a fresh observation.
		delete(w.xrReadyRecorded, key)
	}
}

// extractBindings returns the set of "binding_type/provider_name" keys from an
// Api's spec.parameters refs (sqlRef, nosqlRef, topicRef, subscriptionRef,
// objectStorageRefs). The returned keys are used by syncBindings and clearBindings
// to delete stale Prometheus label sets.
func extractBindings(obj *unstructured.Unstructured) map[string]struct{} {
	result := make(map[string]struct{})
	if n, _, _ := unstructured.NestedString(obj.Object, "spec", "parameters", "sqlRef", "name"); n != "" {
		result["sql/"+n] = struct{}{}
	}
	if n, _, _ := unstructured.NestedString(obj.Object, "spec", "parameters", "nosqlRef", "name"); n != "" {
		result["nosql/"+n] = struct{}{}
	}
	if n, _, _ := unstructured.NestedString(obj.Object, "spec", "parameters", "topicRef", "name"); n != "" {
		result["topic/"+n] = struct{}{}
	}
	if n, _, _ := unstructured.NestedString(obj.Object, "spec", "parameters", "subscriptionRef", "name"); n != "" {
		result["subscription/"+n] = struct{}{}
	}
	refs, _, _ := unstructured.NestedSlice(obj.Object, "spec", "parameters", "objectStorageRefs")
	for _, ref := range refs {
		rm, ok := ref.(map[string]interface{})
		if !ok {
			continue
		}
		if n, _ := rm["name"].(string); n != "" {
			result["object-storage/"+n] = struct{}{}
		}
	}
	return result
}

// syncBindings diffs newBindings against the previously recorded set for
// consumerName, deletes stale gauges, and sets gauges for active bindings.
func (w *watcher) syncBindings(consumerName, consumerKind string, newBindings map[string]struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	old := w.xrBindings[consumerName]
	for k := range old {
		if _, ok := newBindings[k]; !ok {
			parts := strings.SplitN(k, "/", 2)
			xrBinding.DeleteLabelValues(consumerKind, consumerName, parts[0], parts[1])
		}
	}
	for k := range newBindings {
		parts := strings.SplitN(k, "/", 2)
		xrBinding.WithLabelValues(consumerKind, consumerName, parts[0], parts[1]).Set(1)
	}
	w.xrBindings[consumerName] = newBindings
}

// clearBindings removes all binding gauges for a deleted Api.
func (w *watcher) clearBindings(consumerName, consumerKind string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k := range w.xrBindings[consumerName] {
		parts := strings.SplitN(k, "/", 2)
		xrBinding.DeleteLabelValues(consumerKind, consumerName, parts[0], parts[1])
	}
	delete(w.xrBindings, consumerName)
}

// ── Managed resource watching (IAM Role, RolesAnywhere Profile) ──────────────

func (w *watcher) watchManaged(ctx context.Context, k managedKind) {
	gvr := schema.GroupVersionResource{Group: k.group, Version: k.version, Resource: k.resource}
	retryWatch(ctx, k.kind, func() error { return w.doWatchManaged(ctx, gvr, k) })
}

func (w *watcher) doWatchManaged(ctx context.Context, gvr schema.GroupVersionResource, k managedKind) error {
	list, err := w.client.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	w.reconcileManaged(list.Items, k)

	wi, err := w.client.Resource(gvr).Namespace("").Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
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
			if ev.Type == watch.Deleted {
				if obj, ok := ev.Object.(*unstructured.Unstructured); ok {
					w.deleteManaged(k.kind + "/" + obj.GetName())
				}
				continue
			}
			if ev.Type != watch.Added && ev.Type != watch.Modified {
				continue
			}
			obj, ok := ev.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			w.handleManaged(obj, k)
		}
	}
}

// reconcileManaged mirrors reconcileXR for managed resources: refresh gauges
// for everything currently listed, and clear gauges for anything this
// watcher previously tracked (of the same kind) that has since disappeared.
func (w *watcher) reconcileManaged(items []unstructured.Unstructured, k managedKind) {
	seen := make(map[string]struct{}, len(items))
	for i := range items {
		obj := &items[i]
		seen[k.kind+"/"+obj.GetName()] = struct{}{}
		w.handleManaged(obj, k)
	}

	w.mu.Lock()
	var stale []string
	for key, id := range w.managedLive {
		if id.kind != k.kind {
			continue
		}
		if _, ok := seen[key]; !ok {
			stale = append(stale, key)
		}
	}
	w.mu.Unlock()

	for _, key := range stale {
		w.deleteManaged(key)
	}
}

// deleteManaged clears every gauge tracked for a given managed-resource key ("kind/name").
func (w *watcher) deleteManaged(key string) {
	w.mu.Lock()
	id, ok := w.managedLive[key]
	if ok {
		delete(w.managedLive, key)
		delete(w.managedReadyRecorded, key)
	}
	w.mu.Unlock()
	if !ok {
		return
	}
	managedReady.DeleteLabelValues(id.kind, id.name, id.ns)
	managedReadyDuration.DeleteLabelValues(id.kind, id.name, id.ns)
}

func (w *watcher) handleManaged(obj *unstructured.Unstructured, k managedKind) {
	name := obj.GetName()
	ns := obj.GetNamespace()
	created := obj.GetCreationTimestamp().Time
	key := k.kind + "/" + name

	ready, readyAt := readyCondition(obj)
	gauge := 0.0
	if ready {
		gauge = 1.0
	}
	managedReady.WithLabelValues(k.kind, name, ns).Set(gauge)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.managedLive[key] = managedIdentity{kind: k.kind, name: name, ns: ns}

	if ready && !readyAt.IsZero() {
		if !w.managedReadyRecorded[key] {
			w.managedReadyRecorded[key] = true
			if created.After(w.startedAt) {
				elapsed := readyAt.Sub(created).Seconds()
				managedReadyDuration.WithLabelValues(k.kind, name, ns).Set(elapsed)
				managedTimeToReady.WithLabelValues(k.kind).Observe(elapsed)
				slog.Info("managed ready", "kind", k.kind, "name", name, "seconds", elapsed)
			}
		}
	}
	if !ready {
		// Clear so a ready→not-ready→ready flip records a fresh observation.
		delete(w.managedReadyRecorded, key)
	}
}

// ── Shared helpers ────────────────────────────────────────────────────────────

// readyCondition returns whether obj has a Ready=True condition and when it last
// transitioned. If lastTransitionTime is absent or unparseable the second return
// is the zero Time; callers guard with !readyAt.IsZero() before using the duration.
func readyCondition(obj *unstructured.Unstructured) (bool, time.Time) {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cm["type"] != "Ready" {
			continue
		}
		if cm["status"] != "True" {
			return false, time.Time{}
		}
		t, _ := cm["lastTransitionTime"].(string)
		parsed, _ := time.Parse(time.RFC3339, t)
		return true, parsed
	}
	return false, time.Time{}
}

// backendOf returns the infrastructure backend label for an XR. It reads
// spec.parameters.backend when set; otherwise NoSql and ObjectStorage are
// always provisioned on AWS (public-cloud) while all other kinds default to the
// private cluster (private-cloud).
func backendOf(obj *unstructured.Unstructured, kind string) string {
	backend, _, _ := unstructured.NestedString(obj.Object, "spec", "parameters", "backend")
	if backend != "" {
		return backend
	}
	switch kind {
	case "NoSql", "ObjectStorage":
		return "public-cloud"
	}
	return "private-cloud"
}

// retryWatch calls fn in a loop, restarting after a 5 s backoff on both errors
// and clean channel closes. The API server routinely terminates watches after a
// few minutes (bookmark timeout), so a nil return is not treated as fatal.
func retryWatch(ctx context.Context, name string, fn func() error) {
	for {
		if err := fn(); err != nil {
			slog.Warn("watch error, retrying", "resource", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
