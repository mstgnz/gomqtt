package ratelimit

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// RateLimitConfig represents the rate limit plugin configuration
type RateLimitConfig struct {
	ConnectionRate int      `json:"connection_rate"`
	PublishRate    int      `json:"publish_rate"`
	SubscribeRate  int      `json:"subscribe_rate"`
	ByteRate       int      `json:"byte_rate"`
	WindowSize     int      `json:"window_size"`
	IPWhitelist    []string `json:"ip_whitelist"`
}

// RateLimitPlugin is a plugin that limits connection and message rates
type RateLimitPlugin struct {
	*plugin.BasePlugin
	config             *RateLimitConfig
	connectionCounters map[string]*counter
	publishCounters    map[string]*counter
	subscribeCounters  map[string]*counter
	byteCounters       map[string]*counter
	ipWhitelist        []*net.IPNet
	mutex              sync.RWMutex
	active             bool
}

// counter tracks event occurrences within a time window
type counter struct {
	count      int
	bytes      int
	lastUpdate time.Time
	mutex      sync.Mutex
}

// NewRateLimitPlugin creates a new rate limiting plugin
func NewRateLimitPlugin() *RateLimitPlugin {
	p := &RateLimitPlugin{
		BasePlugin:         plugin.NewBasePlugin("ratelimit", "Rate limiting for connections and messages", "1.0.0", "GoMQTT Team"),
		connectionCounters: make(map[string]*counter),
		publishCounters:    make(map[string]*counter),
		subscribeCounters:  make(map[string]*counter),
		byteCounters:       make(map[string]*counter),
		active:             true,
	}

	// Register event handlers
	p.RegisterEventHandler(plugin.EventClientConnect, p.handleClientConnect)
	p.RegisterEventHandler(plugin.EventMessagePublish, p.handleMessagePublish)
	p.RegisterEventHandler(plugin.EventSubscribe, p.handleSubscribe)

	return p
}

// Initialize initializes the rate limit plugin
func (p *RateLimitPlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*RateLimitConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	p.config = config

	// Parse IP whitelist
	for _, cidr := range config.IPWhitelist {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			// Try as a single IP address
			ip := net.ParseIP(cidr)
			if ip == nil {
				log.Printf("Invalid IP or CIDR in whitelist: %s", cidr)
				continue
			}

			// Convert to CIDR with full mask
			if ip.To4() != nil {
				ipNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}
			} else {
				ipNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
			}
		}
		p.ipWhitelist = append(p.ipWhitelist, ipNet)
	}

	log.Printf("Rate limit plugin initialized with connection_rate=%d, publish_rate=%d, subscribe_rate=%d, byte_rate=%d",
		config.ConnectionRate, config.PublishRate, config.SubscribeRate, config.ByteRate)
	return nil
}

// isWhitelisted checks if an IP is in the whitelist
func (p *RateLimitPlugin) isWhitelisted(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	for _, ipNet := range p.ipWhitelist {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// handleClientConnect handles client connect events
func (p *RateLimitPlugin) handleClientConnect(ctx *plugin.Context) error {
	if !p.active || p.config.ConnectionRate <= 0 {
		return nil
	}

	// Extract client IP from properties
	ipStr, ok := ctx.Properties["ip"].(string)
	if !ok {
		return nil
	}

	// Skip whitelisted IPs
	if p.isWhitelisted(ipStr) {
		return nil
	}

	// Get or create counter for this IP
	p.mutex.Lock()
	c, exists := p.connectionCounters[ipStr]
	if !exists {
		c = &counter{lastUpdate: time.Now()}
		p.connectionCounters[ipStr] = c
	}
	p.mutex.Unlock()

	// Update the counter
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Reset counter if window has elapsed
	windowSize := p.config.WindowSize
	if windowSize <= 0 {
		windowSize = 60 // Default to 60 seconds
	}

	if time.Since(c.lastUpdate).Seconds() > float64(windowSize) {
		c.count = 0
		c.lastUpdate = time.Now()
	}

	// Check rate limit
	c.count++
	if c.count > p.config.ConnectionRate {
		return fmt.Errorf("connection rate limit exceeded for IP %s", ipStr)
	}

	return nil
}

// handleMessagePublish handles message publish events
func (p *RateLimitPlugin) handleMessagePublish(ctx *plugin.Context) error {
	if !p.active || (p.config.PublishRate <= 0 && p.config.ByteRate <= 0) {
		return nil
	}

	// Skip rate limiting if client ID is not set
	if ctx.ClientID == "" {
		return nil
	}

	// Check publish rate
	if p.config.PublishRate > 0 {
		p.mutex.Lock()
		c, exists := p.publishCounters[ctx.ClientID]
		if !exists {
			c = &counter{lastUpdate: time.Now()}
			p.publishCounters[ctx.ClientID] = c
		}
		p.mutex.Unlock()

		c.mutex.Lock()
		defer c.mutex.Unlock()

		// Reset counter if window has elapsed
		windowSize := p.config.WindowSize
		if windowSize <= 0 {
			windowSize = 60
		}

		if time.Since(c.lastUpdate).Seconds() > float64(windowSize) {
			c.count = 0
			c.lastUpdate = time.Now()
		}

		// Check rate limit
		c.count++
		if c.count > p.config.PublishRate {
			return fmt.Errorf("publish rate limit exceeded for client %s", ctx.ClientID)
		}
	}

	// Check byte rate
	if p.config.ByteRate > 0 && ctx.Payload != nil {
		p.mutex.Lock()
		c, exists := p.byteCounters[ctx.ClientID]
		if !exists {
			c = &counter{lastUpdate: time.Now()}
			p.byteCounters[ctx.ClientID] = c
		}
		p.mutex.Unlock()

		c.mutex.Lock()
		defer c.mutex.Unlock()

		// Reset counter if window has elapsed
		windowSize := p.config.WindowSize
		if windowSize <= 0 {
			windowSize = 60
		}

		if time.Since(c.lastUpdate).Seconds() > float64(windowSize) {
			c.bytes = 0
			c.lastUpdate = time.Now()
		}

		// Check byte rate limit
		c.bytes += len(ctx.Payload)
		if c.bytes > p.config.ByteRate {
			return fmt.Errorf("byte rate limit exceeded for client %s", ctx.ClientID)
		}
	}

	return nil
}

// handleSubscribe handles subscribe events
func (p *RateLimitPlugin) handleSubscribe(ctx *plugin.Context) error {
	if !p.active || p.config.SubscribeRate <= 0 {
		return nil
	}

	// Skip rate limiting if client ID is not set
	if ctx.ClientID == "" {
		return nil
	}

	// Get or create counter for this client
	p.mutex.Lock()
	c, exists := p.subscribeCounters[ctx.ClientID]
	if !exists {
		c = &counter{lastUpdate: time.Now()}
		p.subscribeCounters[ctx.ClientID] = c
	}
	p.mutex.Unlock()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Reset counter if window has elapsed
	windowSize := p.config.WindowSize
	if windowSize <= 0 {
		windowSize = 60
	}

	if time.Since(c.lastUpdate).Seconds() > float64(windowSize) {
		c.count = 0
		c.lastUpdate = time.Now()
	}

	// Check rate limit
	c.count++
	if c.count > p.config.SubscribeRate {
		return fmt.Errorf("subscribe rate limit exceeded for client %s", ctx.ClientID)
	}

	return nil
}

// cleanup periodically cleans up expired counters
func (p *RateLimitPlugin) cleanup() {
	for p.active {
		time.Sleep(time.Duration(p.config.WindowSize) * time.Second)

		p.mutex.Lock()
		// Clean up connection counters
		for ip, c := range p.connectionCounters {
			c.mutex.Lock()
			if time.Since(c.lastUpdate).Seconds() > float64(p.config.WindowSize*2) {
				delete(p.connectionCounters, ip)
			}
			c.mutex.Unlock()
		}

		// Clean up publish counters
		for clientID, c := range p.publishCounters {
			c.mutex.Lock()
			if time.Since(c.lastUpdate).Seconds() > float64(p.config.WindowSize*2) {
				delete(p.publishCounters, clientID)
			}
			c.mutex.Unlock()
		}

		// Clean up subscribe counters
		for clientID, c := range p.subscribeCounters {
			c.mutex.Lock()
			if time.Since(c.lastUpdate).Seconds() > float64(p.config.WindowSize*2) {
				delete(p.subscribeCounters, clientID)
			}
			c.mutex.Unlock()
		}

		// Clean up byte counters
		for clientID, c := range p.byteCounters {
			c.mutex.Lock()
			if time.Since(c.lastUpdate).Seconds() > float64(p.config.WindowSize*2) {
				delete(p.byteCounters, clientID)
			}
			c.mutex.Unlock()
		}
		p.mutex.Unlock()
	}
}

// Shutdown stops the rate limit plugin
func (p *RateLimitPlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()
	log.Printf("Rate limit plugin shut down")
	return nil
}

// New creates a new rate limit plugin instance
func New() any {
	return NewRateLimitPlugin()
}
