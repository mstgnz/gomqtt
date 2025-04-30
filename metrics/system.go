package metrics

import (
	"runtime"
	"time"
)

// StartSystemMetricsCollector starts a goroutine that periodically collects system metrics
func StartSystemMetricsCollector(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			updateSystemMetrics()
		}
	}()
}

// updateSystemMetrics updates CPU and memory usage metrics
func updateSystemMetrics() {
	// Update memory metrics using the runtime package
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Update memory usage (Alloc shows current heap memory allocated)
	SystemMemoryUsage.Set(float64(memStats.Alloc))

	// CPU metrics from runtime are not directly available in Go
	// This is a simplified version that uses goroutines count as indicator
	// For accurate CPU usage, you'd typically use a package like gopsutil
	SystemCPUUsage.Set(float64(runtime.NumGoroutine()))
}
