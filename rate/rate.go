package rate

import (
	"sync"
	"time"
)

// Limiter implements token bucket algorithm for rate limiting
type Limiter struct {
	// Rate is the number of tokens per second
	rate float64
	// Capacity is the maximum number of tokens the bucket can hold
	capacity float64
	// Tokens is the current number of tokens in the bucket
	tokens float64
	// LastRefill is the last time the bucket was refilled
	lastRefill time.Time
	// Mutex to protect concurrent access
	mu sync.Mutex
}

// New creates a new rate limiter with the given rate and capacity
func New(rate float64, capacity float64) *Limiter {
	return &Limiter{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// Allow checks if an action is allowed by the rate limiter
// Returns true if the action is allowed, false otherwise
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Refill tokens based on time elapsed since last refill
	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.tokens = min(l.capacity, l.tokens+elapsed*l.rate)
	l.lastRefill = now

	// Check if we have enough tokens
	if l.tokens >= 1 {
		l.tokens -= 1
		return true
	}

	return false
}

// Reset resets the limiter to its initial state
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.tokens = l.capacity
	l.lastRefill = time.Now()
}

// ClientLimiter manages rate limiters for each client
type ClientLimiter struct {
	// Map of client ID to map of limit types to limiters
	limiters map[string]map[string]*Limiter
	// Default rates for different limit types
	defaultRates map[string]float64
	// Default burst multiplier
	burstMultiplier float64
	// Mutex for thread safety
	mu sync.RWMutex
}

// NewClientLimiter creates a new client rate limiter
func NewClientLimiter(defaultRates map[string]float64, burstMultiplier float64) *ClientLimiter {
	return &ClientLimiter{
		limiters:        make(map[string]map[string]*Limiter),
		defaultRates:    defaultRates,
		burstMultiplier: burstMultiplier,
	}
}

// Allow checks if an operation is allowed for a client
func (c *ClientLimiter) Allow(clientID string, limitType string) bool {
	// Get or create the client's limiters
	c.mu.RLock()
	clientLimiters, exists := c.limiters[clientID]
	c.mu.RUnlock()

	if !exists {
		// Create new limiters for this client
		c.mu.Lock()
		// Double check in case another goroutine created it
		if clientLimiters, exists = c.limiters[clientID]; !exists {
			clientLimiters = make(map[string]*Limiter)
			c.limiters[clientID] = clientLimiters
		}
		c.mu.Unlock()
	}

	// Get or create the specific limiter
	c.mu.RLock()
	limiter, exists := clientLimiters[limitType]
	c.mu.RUnlock()

	if !exists {
		// Create the limiter
		c.mu.Lock()
		// Double check in case another goroutine created it
		if limiter, exists = clientLimiters[limitType]; !exists {
			rate := c.defaultRates[limitType]
			if rate <= 0 {
				// Default fallback rate if not specified
				rate = 10
			}
			limiter = New(rate, rate*c.burstMultiplier)
			clientLimiters[limitType] = limiter
		}
		c.mu.Unlock()
	}

	return limiter.Allow()
}

// SetClientRate sets a custom rate for a specific client and limit type
func (c *ClientLimiter) SetClientRate(clientID string, limitType string, rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create client entry if it doesn't exist
	if _, exists := c.limiters[clientID]; !exists {
		c.limiters[clientID] = make(map[string]*Limiter)
	}

	// Create new limiter with custom rate
	c.limiters[clientID][limitType] = New(rate, rate*c.burstMultiplier)
}

// SetDefaultRate sets the default rate for a limit type
func (c *ClientLimiter) SetDefaultRate(limitType string, rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultRates[limitType] = rate
}

// Reset resets rate limits for a client
func (c *ClientLimiter) Reset(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.limiters, clientID)
}

// ResetAll resets all rate limits
func (c *ClientLimiter) ResetAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limiters = make(map[string]map[string]*Limiter)
}

// Common limit types
const (
	ConnectLimit     = "connect"
	PublishLimit     = "publish"
	SubscribeLimit   = "subscribe"
	MessageSizeLimit = "message_size"
)

// Utility function
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
