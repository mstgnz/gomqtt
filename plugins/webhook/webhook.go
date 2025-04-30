package webhook

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// WebhookPlugin is a plugin that sends webhook notifications when messages are published
type WebhookPlugin struct {
	plugin     *plugin.Plugin
	endpoints  map[string]string // topic pattern -> webhook URL
	httpClient *http.Client
	mutex      sync.RWMutex
}

// WebhookPayload represents the JSON payload sent to webhook endpoints
type WebhookPayload struct {
	ClientID  string            `json:"client_id"`
	Topic     string            `json:"topic"`
	Payload   string            `json:"payload"`
	QoS       byte              `json:"qos"`
	Retained  bool              `json:"retained"`
	Timestamp int64             `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// NewWebhookPlugin creates a new webhook plugin
func NewWebhookPlugin() *WebhookPlugin {
	p := &WebhookPlugin{
		plugin: plugin.NewPlugin(
			"webhook",
			"Sends HTTP webhooks when messages are published",
			"1.0.0",
			"GoMQTT Team",
		),
		endpoints: make(map[string]string),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Register handlers
	p.plugin.OnEvent(plugin.EventMessagePublish, p.handlePublish)

	return p
}

// Plugin returns the underlying plugin
func (p *WebhookPlugin) Plugin() *plugin.Plugin {
	return p.plugin
}

// RegisterEndpoint registers a webhook endpoint for a topic pattern
func (p *WebhookPlugin) RegisterEndpoint(topicPattern, webhookURL string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.endpoints[topicPattern] = webhookURL
	log.Printf("Registered webhook for topic '%s' to URL '%s'", topicPattern, webhookURL)
}

// UnregisterEndpoint removes a webhook endpoint for a topic pattern
func (p *WebhookPlugin) UnregisterEndpoint(topicPattern string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.endpoints, topicPattern)
	log.Printf("Unregistered webhook for topic '%s'", topicPattern)
}

// handlePublish handles message publish events
func (p *WebhookPlugin) handlePublish(ctx *plugin.Context) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Check for matching endpoints
	for pattern, url := range p.endpoints {
		// Simple exact topic match for now, could be enhanced with wildcards
		if pattern == ctx.Topic || pattern == "#" {
			go p.sendWebhook(url, ctx)
		}
	}

	return nil
}

// sendWebhook sends a webhook notification
func (p *WebhookPlugin) sendWebhook(url string, ctx *plugin.Context) {
	payload := WebhookPayload{
		ClientID:  ctx.ClientID,
		Topic:     ctx.Topic,
		Payload:   string(ctx.Payload),
		QoS:       ctx.QoS,
		Retained:  ctx.Retained,
		Timestamp: ctx.Timestamp,
		Headers: map[string]string{
			"Content-Type": "application/json",
			"User-Agent":   "GoMQTT-Webhook/1.0",
		},
	}

	// Convert to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling webhook payload: %v", err)
		return
	}

	// Send HTTP request
	resp, err := p.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error sending webhook to %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	log.Printf("Webhook sent to %s with status code %d", url, resp.StatusCode)
}

// GetEndpoints returns the current webhook endpoints
func (p *WebhookPlugin) GetEndpoints() map[string]string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Create a copy of the endpoints map to avoid race conditions
	endpointsCopy := make(map[string]string, len(p.endpoints))
	for k, v := range p.endpoints {
		endpointsCopy[k] = v
	}

	return endpointsCopy
}
