package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// WebhookConfig represents the webhook plugin configuration
type WebhookConfig struct {
	Endpoints     []EndpointConfig `json:"endpoints"`
	Timeout       int              `json:"timeout"`
	RetryCount    int              `json:"retry_count"`
	RetryInterval int              `json:"retry_interval"`
	MaxConcurrent int              `json:"max_concurrent"`
}

// EndpointConfig represents a webhook endpoint configuration
type EndpointConfig struct {
	URL         string            `json:"url"`
	TopicFilter string            `json:"topic_filter"`
	Method      string            `json:"method"`
	QoS         byte              `json:"qos"`
	Headers     map[string]string `json:"headers"`
	Template    string            `json:"template"`
	Enabled     bool              `json:"enabled"`
}

// WebhookPlugin is a plugin that sends webhook notifications when messages are published
type WebhookPlugin struct {
	*plugin.BasePlugin
	endpoints     []EndpointConfig
	httpClient    *http.Client
	config        *WebhookConfig
	mutex         sync.RWMutex
	topicMatchers map[string]*regexp.Regexp
	active        bool
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
		BasePlugin: plugin.NewBasePlugin(
			"webhook",
			"Sends HTTP webhooks when messages are published",
			"1.0.0",
			"GoMQTT Team",
		),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		topicMatchers: make(map[string]*regexp.Regexp),
		active:        true,
	}

	// Register handlers
	p.RegisterEventHandler(plugin.EventMessagePublish, p.handlePublish)

	return p
}

// Initialize initializes the webhook plugin
func (p *WebhookPlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*WebhookConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	p.config = config

	// Set timeout based on configuration
	if config.Timeout > 0 {
		p.httpClient.Timeout = time.Duration(config.Timeout) * time.Second
	}

	// Configure endpoints
	p.mutex.Lock()
	p.endpoints = config.Endpoints
	p.mutex.Unlock()

	// Precompile topic matchers
	for _, endpoint := range config.Endpoints {
		if endpoint.Enabled {
			// Convert MQTT wildcards to regex
			pattern := endpoint.TopicFilter
			pattern = regexp.QuoteMeta(pattern)
			pattern = regexp.MustCompile(`\\\+`).ReplaceAllString(pattern, "[^/]+")
			pattern = regexp.MustCompile(`\\\#`).ReplaceAllString(pattern, ".*")
			pattern = "^" + pattern + "$"

			// Compile regex
			regex, err := regexp.Compile(pattern)
			if err != nil {
				log.Printf("Invalid topic filter '%s': %v", endpoint.TopicFilter, err)
				continue
			}

			p.topicMatchers[endpoint.TopicFilter] = regex
		}
	}

	log.Printf("Webhook plugin initialized with %d endpoints", len(config.Endpoints))
	return nil
}

// handlePublish handles message publish events
func (p *WebhookPlugin) handlePublish(ctx *plugin.Context) error {
	if !p.active {
		return nil
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Check for matching endpoints
	for _, endpoint := range p.endpoints {
		if !endpoint.Enabled {
			continue
		}

		// Check QoS level
		if ctx.QoS < endpoint.QoS {
			continue
		}

		// Check if topic matches the filter
		if matcher, ok := p.topicMatchers[endpoint.TopicFilter]; ok {
			if matcher.MatchString(ctx.Topic) {
				// Don't block on webhook sending
				go p.sendWebhook(endpoint, ctx)
			}
		}
	}

	return nil
}

// sendWebhook sends a webhook notification
func (p *WebhookPlugin) sendWebhook(endpoint EndpointConfig, ctx *plugin.Context) {
	payload := WebhookPayload{
		ClientID:  ctx.ClientID,
		Topic:     ctx.Topic,
		Payload:   string(ctx.Payload),
		QoS:       ctx.QoS,
		Retained:  ctx.Retained,
		Timestamp: ctx.Timestamp,
		Headers:   make(map[string]string),
	}

	// Add default headers
	payload.Headers = map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "GoMQTT-Webhook/1.0",
	}

	// Add custom headers from configuration
	for k, v := range endpoint.Headers {
		payload.Headers[k] = v
	}

	// Convert to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling webhook payload: %v", err)
		return
	}

	// Determine HTTP method
	method := endpoint.Method
	if method == "" {
		method = "POST"
	}

	// Create the request
	req, err := http.NewRequest(method, endpoint.URL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error creating webhook request: %v", err)
		return
	}

	// Set headers
	for k, v := range payload.Headers {
		req.Header.Set(k, v)
	}

	// Send HTTP request with retries
	var resp *http.Response
	retries := 0
	maxRetries := p.config.RetryCount
	if maxRetries <= 0 {
		maxRetries = 3
	}

	retryInterval := p.config.RetryInterval
	if retryInterval <= 0 {
		retryInterval = 5
	}

	for retries <= maxRetries {
		resp, err = p.httpClient.Do(req)
		if err == nil && resp.StatusCode < 500 {
			break
		}

		if err != nil {
			log.Printf("Webhook request failed (attempt %d/%d): %v",
				retries+1, maxRetries+1, err)
		} else {
			log.Printf("Webhook request returned error status %d (attempt %d/%d)",
				resp.StatusCode, retries+1, maxRetries+1)
			resp.Body.Close()
		}

		retries++
		if retries <= maxRetries {
			time.Sleep(time.Duration(retryInterval) * time.Second)
		}
	}

	if resp != nil {
		defer resp.Body.Close()
		log.Printf("Webhook sent to %s with status code %d", endpoint.URL, resp.StatusCode)
	}
}

// Shutdown stops the webhook plugin
func (p *WebhookPlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()
	log.Printf("Webhook plugin shut down")
	return nil
}

// New creates a new webhook plugin instance - this function is used when loading the plugin externally
func New() any {
	return NewWebhookPlugin()
}
