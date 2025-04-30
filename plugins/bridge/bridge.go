package bridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// BridgeConfig represents the bridge plugin configuration
type BridgeConfig struct {
	Bridges []BridgeEndpoint `json:"bridges"`
	Timeout int              `json:"timeout"`
}

// BridgeEndpoint represents a bridge endpoint configuration
type BridgeEndpoint struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"` // "http", "http-ws", "mqtt", "coap", "grpc", "amqp", "kafka"
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	TopicFilter string            `json:"topic_filter"`
	QoS         byte              `json:"qos"`
	Headers     map[string]string `json:"headers"`
	Username    string            `json:"username"`
	Password    string            `json:"password"`
	Enabled     bool              `json:"enabled"`
	// CoAP specific
	CoAPPort int `json:"coap_port"`
	// MQTT Bridge specific
	ClientID     string `json:"client_id"`
	TargetTopic  string `json:"target_topic"`
	CleanSession bool   `json:"clean_session"`
	// AMQP specific
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
	// Kafka specific
	KafkaTopic  string `json:"kafka_topic"`
	KafkaBroker string `json:"kafka_broker"`
	// gRPC specific
	GRPCService string `json:"grpc_service"`
	GRPCMethod  string `json:"grpc_method"`
}

// BridgePlugin is a plugin that bridges MQTT messages to other protocols
type BridgePlugin struct {
	*plugin.BasePlugin
	config     *BridgeConfig
	httpClient *http.Client
	mutex      sync.RWMutex
	active     bool
}

// NewBridgePlugin creates a new bridge plugin
func NewBridgePlugin() *BridgePlugin {
	p := &BridgePlugin{
		BasePlugin: plugin.NewBasePlugin(
			"bridge",
			"Bridges MQTT messages to other protocols",
			"1.0.0",
			"GoMQTT Team",
		),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		active: true,
	}

	// Register event handlers
	p.RegisterEventHandler(plugin.EventMessagePublish, p.handlePublish)

	return p
}

// Initialize initializes the bridge plugin
func (p *BridgePlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*BridgeConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	p.config = config

	// Set timeout based on configuration
	if config.Timeout > 0 {
		p.httpClient.Timeout = time.Duration(config.Timeout) * time.Second
	}

	log.Printf("Bridge plugin initialized with %d bridges", len(config.Bridges))
	return nil
}

// handlePublish handles message publish events
func (p *BridgePlugin) handlePublish(ctx *plugin.Context) error {
	if !p.active {
		return nil
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Check for matching bridges
	for _, bridge := range p.config.Bridges {
		if !bridge.Enabled {
			continue
		}

		// Check QoS level
		if ctx.QoS < bridge.QoS {
			continue
		}

		// Simple topic matching (could be enhanced with wildcards)
		if isTopicMatch(bridge.TopicFilter, ctx.Topic) {
			// Don't block on sending
			go p.sendToBridge(bridge, ctx)
		}
	}

	return nil
}

// isTopicMatch checks if a topic matches a filter
func isTopicMatch(filter, topic string) bool {
	// Exact match
	if filter == topic {
		return true
	}

	// # wildcard (match all)
	if filter == "#" {
		return true
	}

	// Topic level wildcards (simplified implementation)
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	if len(filterParts) > len(topicParts) {
		return false
	}

	for i, part := range filterParts {
		if part == "#" {
			return true
		}
		if part == "+" {
			continue
		}
		if i >= len(topicParts) || part != topicParts[i] {
			return false
		}
	}

	return len(filterParts) == len(topicParts)
}

// sendToBridge sends a message to the appropriate bridge
func (p *BridgePlugin) sendToBridge(bridge BridgeEndpoint, ctx *plugin.Context) {
	bridgeType := strings.ToLower(bridge.Type)

	switch bridgeType {
	case "http", "http-ws":
		p.sendToHTTP(bridge, ctx)
	case "mqtt":
		p.sendToMQTT(bridge, ctx)
	case "coap":
		p.sendToCoAP(bridge, ctx)
	case "grpc":
		p.sendToGRPC(bridge, ctx)
	case "amqp":
		p.sendToAMQP(bridge, ctx)
	case "kafka":
		p.sendToKafka(bridge, ctx)
	default:
		log.Printf("Unsupported bridge type: %s", bridge.Type)
	}
}

// sendToHTTP sends a message to an HTTP endpoint
func (p *BridgePlugin) sendToHTTP(bridge BridgeEndpoint, ctx *plugin.Context) {
	// Create payload for HTTP
	payload := map[string]any{
		"topic":     ctx.Topic,
		"payload":   string(ctx.Payload),
		"qos":       ctx.QoS,
		"retained":  ctx.Retained,
		"client_id": ctx.ClientID,
		"timestamp": ctx.Timestamp,
	}

	// Convert to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling HTTP payload for bridge %s: %v", bridge.Name, err)
		return
	}

	// Determine HTTP method
	method := bridge.Method
	if method == "" {
		method = "POST"
	}

	// Create the request
	req, err := http.NewRequest(method, bridge.URL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error creating HTTP request for bridge %s: %v", bridge.Name, err)
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "GoMQTT-Bridge/1.0")
	for k, v := range bridge.Headers {
		req.Header.Set(k, v)
	}

	// Set basic auth if provided
	if bridge.Username != "" && bridge.Password != "" {
		req.SetBasicAuth(bridge.Username, bridge.Password)
	}

	// Send HTTP request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		log.Printf("HTTP bridge request failed for %s: %v", bridge.Name, err)
		return
	}
	defer resp.Body.Close()

	log.Printf("HTTP bridge %s sent message to %s with status code %d", bridge.Name, bridge.URL, resp.StatusCode)
}

// sendToMQTT sends a message to another MQTT broker
func (p *BridgePlugin) sendToMQTT(bridge BridgeEndpoint, ctx *plugin.Context) {
	// This is a placeholder for MQTT bridge implementation
	// In a real implementation, you would:
	// 1. Create an MQTT client (using Paho or similar)
	// 2. Connect to the target broker
	// 3. Publish the message to the target topic

	log.Printf("MQTT bridge %s would send message to %s (not fully implemented)",
		bridge.Name, bridge.URL)
}

// sendToCoAP sends a message to a CoAP endpoint
func (p *BridgePlugin) sendToCoAP(bridge BridgeEndpoint, ctx *plugin.Context) {
	// This is a simplified placeholder for the CoAP bridge
	// The actual implementation would use the CoAPBridge type

	log.Printf("CoAP bridge %s would send message to %s (not fully implemented)",
		bridge.Name, bridge.URL)
}

// sendToGRPC sends a message to a gRPC service
func (p *BridgePlugin) sendToGRPC(bridge BridgeEndpoint, ctx *plugin.Context) {
	// This is a placeholder for gRPC bridge implementation
	// In a real implementation, you would:
	// 1. Create a gRPC client
	// 2. Connect to the target service
	// 3. Call the specified method with the message as payload

	log.Printf("gRPC bridge %s would send message to service %s, method %s (not fully implemented)",
		bridge.Name, bridge.GRPCService, bridge.GRPCMethod)
}

// sendToAMQP sends a message to an AMQP broker (RabbitMQ)
func (p *BridgePlugin) sendToAMQP(bridge BridgeEndpoint, ctx *plugin.Context) {
	// This is a placeholder for AMQP bridge implementation
	// In a real implementation, you would:
	// 1. Create an AMQP client
	// 2. Connect to the broker
	// 3. Publish to the specified exchange with the routing key

	log.Printf("AMQP bridge %s would send message to exchange %s with routing key %s (not fully implemented)",
		bridge.Name, bridge.Exchange, bridge.RoutingKey)
}

// sendToKafka sends a message to a Kafka broker
func (p *BridgePlugin) sendToKafka(bridge BridgeEndpoint, ctx *plugin.Context) {
	// This is a placeholder for Kafka bridge implementation
	// In a real implementation, you would:
	// 1. Create a Kafka producer
	// 2. Connect to the broker
	// 3. Publish the message to the specified topic

	log.Printf("Kafka bridge %s would send message to topic %s on broker %s (not fully implemented)",
		bridge.Name, bridge.KafkaTopic, bridge.KafkaBroker)
}

// Shutdown stops the bridge plugin
func (p *BridgePlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()

	log.Printf("Bridge plugin shut down")
	return nil
}

// New creates a new bridge plugin instance
func New() any {
	return NewBridgePlugin()
}
