package mqtt

import (
	"sync"

	"github.com/mstgnz/gomqtt/rate"
)

// RateLimiter is the manager for rate limiting in the MQTT server
type RateLimiter struct {
	// The client limiter from the rate package
	clientLimiter *rate.ClientLimiter
	// Whether rate limiting is enabled
	enabled bool
	// Mutex for thread safety
	mu sync.RWMutex
}

// NewRateLimiter creates a new rate limiter for the MQTT server
func NewRateLimiter(enabled bool, connectLimit, publishLimit, subscribeLimit, messageSizeLimit float64, burstMultiplier float64) *RateLimiter {
	// Create default rates map
	defaultRates := map[string]float64{
		rate.ConnectLimit:     connectLimit,
		rate.PublishLimit:     publishLimit,
		rate.SubscribeLimit:   subscribeLimit,
		rate.MessageSizeLimit: messageSizeLimit,
	}

	return &RateLimiter{
		clientLimiter: rate.NewClientLimiter(defaultRates, burstMultiplier),
		enabled:       enabled,
	}
}

// Allow checks if an operation is allowed based on rate limits
func (r *RateLimiter) Allow(clientID string, limitType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// If rate limiting is disabled, always allow
	if !r.enabled {
		return true
	}

	return r.clientLimiter.Allow(clientID, limitType)
}

// SetClientRate sets a custom rate for a specific client and limit type
func (r *RateLimiter) SetClientRate(clientID string, limitType string, rate float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clientLimiter.SetClientRate(clientID, limitType, rate)
}

// SetDefaultRate sets the default rate for a limit type
func (r *RateLimiter) SetDefaultRate(limitType string, rate float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clientLimiter.SetDefaultRate(limitType, rate)
}

// Reset resets rate limits for a client
func (r *RateLimiter) Reset(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clientLimiter.Reset(clientID)
}

// Enable enables or disables rate limiting
func (r *RateLimiter) Enable(enabled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enabled = enabled
}

// IsEnabled returns whether rate limiting is enabled
func (r *RateLimiter) IsEnabled() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.enabled
}
