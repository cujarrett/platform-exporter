package main

import "github.com/prometheus/client_golang/prometheus"

var (
	// XR metrics
	xrTimeToReady = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "platform",
		Name:      "xr_time_to_ready_seconds",
		Help:      "Seconds from XR creation to Ready=True, by kind and backend.",
		Buckets:   []float64{5, 15, 30, 60, 120, 300, 600, 900},
	}, []string{"kind", "backend"})

	xrReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "xr_ready",
		Help:      "1 if the XR is Ready, 0 otherwise.",
	}, []string{"kind", "name", "namespace", "backend"})

	xrReadyDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "xr_time_to_ready_seconds_instance",
		Help:      "Seconds from XR creation to Ready=True, per instance.",
	}, []string{"kind", "name", "namespace", "backend"})

	// Managed resource metrics (IAM Role, RolesAnywhere Profile)
	managedTimeToReady = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "platform",
		Name:      "managed_time_to_ready_seconds",
		Help:      "Seconds from managed resource creation to Ready=True, by kind.",
		Buckets:   []float64{5, 15, 30, 60, 120, 300},
	}, []string{"kind"})

	managedReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "managed_ready",
		Help:      "1 if the managed resource is Ready, 0 otherwise.",
	}, []string{"kind", "name", "namespace"})

	managedReadyDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "managed_time_to_ready_seconds_instance",
		Help:      "Seconds from managed resource creation to Ready=True, per instance.",
	}, []string{"kind", "name", "namespace"})

	// Pod init container metrics
	initContainerSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "platform",
		Name:      "pod_init_container_seconds",
		Help:      "Seconds from pod creation to each init container completing, by init container name.",
		Buckets:   []float64{1, 5, 15, 30, 60, 120, 300},
	}, []string{"init_container", "namespace"})

	podInitContainerDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "pod_init_container_seconds_instance",
		Help:      "Seconds from pod creation to each init container completing, per pod.",
	}, []string{"init_container", "namespace", "pod"})

	// Pod ready metrics
	podTimeToReady = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "platform",
		Name:      "pod_time_to_ready_seconds",
		Help:      "Seconds from pod creation to Ready=True.",
		Buckets:   []float64{5, 15, 30, 60, 120, 300, 600},
	}, []string{"namespace"})

	podReadyDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "pod_time_to_ready_seconds_instance",
		Help:      "Seconds from pod creation to Ready=True, per pod.",
	}, []string{"namespace", "pod"})

	// XR binding metrics
	xrBinding = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "platform",
		Name:      "xr_binding",
		Help:      "1 when an XApi has an active binding to a named platform resource.",
	}, []string{"consumer_kind", "consumer_name", "binding_type", "provider_name"})
)

func init() {
	prometheus.MustRegister(
		xrTimeToReady,
		xrReady,
		xrReadyDuration,
		managedTimeToReady,
		managedReady,
		managedReadyDuration,
		initContainerSeconds,
		podInitContainerDuration,
		podTimeToReady,
		podReadyDuration,
		xrBinding,
	)
}
