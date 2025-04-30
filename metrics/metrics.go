// Package metrics provides Prometheus metrics for the MQTT broker
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// General broker metrics
	ConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_connected_clients",
		Help: "The total number of currently connected clients",
	})

	ConnectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_connections_total",
		Help: "The total number of connections since startup",
	})

	MessagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_messages_received_total",
		Help: "The total number of messages received since startup",
	})

	MessagesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_messages_sent_total",
		Help: "The total number of messages sent since startup",
	})

	// Subscription metrics
	ActiveSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_active_subscriptions",
		Help: "The total number of active subscriptions",
	})

	SubscribeOperations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_subscribe_operations_total",
		Help: "The total number of subscribe operations",
	})

	UnsubscribeOperations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_unsubscribe_operations_total",
		Help: "The total number of unsubscribe operations",
	})

	// QoS metrics
	QoSMessagesReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gomqtt_qos_messages_received_total",
			Help: "The total number of messages received by QoS level",
		},
		[]string{"qos"},
	)

	QoSMessagesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gomqtt_qos_messages_sent_total",
			Help: "The total number of messages sent by QoS level",
		},
		[]string{"qos"},
	)

	// Packet metrics by type
	PacketsReceivedByType = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gomqtt_packets_received_by_type",
			Help: "The total number of packets received by type",
		},
		[]string{"type"},
	)

	PacketsSentByType = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gomqtt_packets_sent_by_type",
			Help: "The total number of packets sent by type",
		},
		[]string{"type"},
	)

	// Retained messages
	RetainedMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_retained_messages",
		Help: "The current number of retained messages",
	})

	// System metrics
	SystemMemoryUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_system_memory_bytes",
		Help: "The current memory usage in bytes",
	})

	SystemCPUUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_system_cpu_percent",
		Help: "The current CPU usage percentage",
	})

	// Authentication metrics
	AuthSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_auth_success_total",
		Help: "The total number of successful authentications",
	})

	AuthFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_auth_failure_total",
		Help: "The total number of failed authentications",
	})

	// Message size metrics
	MessageSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gomqtt_message_size_bytes",
		Help:    "Size distribution of messages in bytes",
		Buckets: prometheus.ExponentialBuckets(10, 4, 8), // 10, 40, 160, 640, 2560, 10240, 40960, 163840 bytes
	})

	// Message processing latency
	MessageProcessingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gomqtt_message_processing_seconds",
		Help:    "Message processing time in seconds",
		Buckets: prometheus.LinearBuckets(0.001, 0.001, 10), // 1ms, 2ms, 3ms, ..., 10ms
	})

	// Cluster metrics (if clustering is enabled)
	ClusterNodesActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gomqtt_cluster_nodes_active",
		Help: "The number of active nodes in the cluster",
	})

	ClusterMessageForwardedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gomqtt_cluster_messages_forwarded_total",
		Help: "The total number of messages forwarded to other nodes",
	})
)

// StartMessageProcessingTimer returns a function that, when called,
// records the processing time of a message
func StartMessageProcessingTimer() func() {
	start := time.Now()
	return func() {
		MessageProcessingLatency.Observe(time.Since(start).Seconds())
	}
}

// RecordMessageSize records the size of a message in bytes
func RecordMessageSize(size int) {
	MessageSizeBytes.Observe(float64(size))
}

// IncrementQoSReceived increments the counter for received messages with the specified QoS level
func IncrementQoSReceived(qos byte) {
	QoSMessagesReceivedTotal.WithLabelValues(string(qos + '0')).Inc()
}

// IncrementQoSSent increments the counter for sent messages with the specified QoS level
func IncrementQoSSent(qos byte) {
	QoSMessagesSentTotal.WithLabelValues(string(qos + '0')).Inc()
}

// RecordPacketReceived records a received packet of a specific type
func RecordPacketReceived(packetType string) {
	PacketsReceivedByType.WithLabelValues(packetType).Inc()
}

// RecordPacketSent records a sent packet of a specific type
func RecordPacketSent(packetType string) {
	PacketsSentByType.WithLabelValues(packetType).Inc()
}
