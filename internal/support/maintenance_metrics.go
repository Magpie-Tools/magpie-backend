package support

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var maintenanceLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "magpie_maintenance_last_success_timestamp_seconds", Help: "Unix timestamp of the last successful maintenance pass.",
}, []string{"job"})

var maintenanceFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "magpie_maintenance_failures_total", Help: "Failed maintenance passes.",
}, []string{"job"})

var leaderJobActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "magpie_leader_job_active", Help: "Whether this instance holds leadership for a background job.",
}, []string{"job"})

func init() { prometheus.MustRegister(maintenanceLastSuccess, maintenanceFailures, leaderJobActive) }

func RecordMaintenanceRun(job string, err error) {
	if err != nil {
		maintenanceFailures.WithLabelValues(job).Inc()
		return
	}
	maintenanceLastSuccess.WithLabelValues(job).Set(float64(time.Now().Unix()))
}
