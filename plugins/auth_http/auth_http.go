package auth_http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// AuthConfig holds configuration for the HTTP auth plugin
type AuthConfig struct {
	AuthEndpoint    string            `json:"auth_endpoint"`
	ACLEndpoint     string            `json:"acl_endpoint"`
	Timeout         int               `json:"timeout"`
	CacheExpiry     int               `json:"cache_expiry"`
	Headers         map[string]string `json:"headers"`
	EnableACLCache  bool              `json:"enable_acl_cache"`
	EnableAuthCache bool              `json:"enable_auth_cache"`
}

// AuthRequest represents an authentication request payload
type AuthRequest struct {
	ClientID  string `json:"client_id"`
	Username  string `json:"username"`
	Password  string `json:"password,omitempty"`
	IPAddress string `json:"ip_address,omitempty"`
}

// AuthResponse represents an authentication response
type AuthResponse struct {
	Authenticated bool   `json:"authenticated"`
	Error         string `json:"error,omitempty"`
}

// ACLRequest represents an access control request payload
type ACLRequest struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Topic    string `json:"topic"`
	Action   string `json:"action"` // "publish" or "subscribe"
}

// ACLResponse represents an access control response
type ACLResponse struct {
	Allowed bool   `json:"allowed"`
	Error   string `json:"error,omitempty"`
}

// cacheEntry represents a cached authentication result
type cacheEntry struct {
	result     bool
	expiration time.Time
}

// HTTPAuthPlugin provides HTTP-based authentication and authorization
type HTTPAuthPlugin struct {
	*plugin.BasePlugin
	config     *AuthConfig
	httpClient *http.Client
	authCache  map[string]cacheEntry
	aclCache   map[string]cacheEntry
	mutex      sync.RWMutex
	active     bool
}

// NewHTTPAuthPlugin creates a new HTTP authentication plugin
func NewHTTPAuthPlugin() *HTTPAuthPlugin {
	p := &HTTPAuthPlugin{
		BasePlugin: plugin.NewBasePlugin(
			"auth_http",
			"HTTP-based authentication and authorization",
			"1.0.0",
			"GoMQTT Team",
		),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		authCache: make(map[string]cacheEntry),
		aclCache:  make(map[string]cacheEntry),
		active:    true,
	}

	// Register event handlers
	const EventClientAuthenticate = "client.authenticate"
	const EventACLCheck = "acl.check"
	p.RegisterEventHandler(EventClientAuthenticate, p.handleAuthenticate)
	p.RegisterEventHandler(EventACLCheck, p.handleACLCheck)

	return p
}

// Initialize initializes the plugin with configuration
func (p *HTTPAuthPlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*AuthConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	p.config = config

	// Set timeout based on configuration
	if config.Timeout > 0 {
		p.httpClient.Timeout = time.Duration(config.Timeout) * time.Second
	}

	log.Printf("HTTP Auth plugin initialized with auth endpoint: %s, acl endpoint: %s",
		config.AuthEndpoint, config.ACLEndpoint)

	return nil
}

// handleAuthenticate handles client authentication events
func (p *HTTPAuthPlugin) handleAuthenticate(ctx *plugin.Context) error {
	if !p.active || p.config.AuthEndpoint == "" {
		return nil
	}

	// Extract authentication credentials
	username := ctx.Username
	clientID := ctx.ClientID
	password, _ := ctx.Properties["password"].(string)
	ipAddress, _ := ctx.Properties["ip"].(string)

	// Check cache first
	if p.config.EnableAuthCache && p.config.CacheExpiry > 0 {
		cacheKey := fmt.Sprintf("%s:%s:%s", clientID, username, password)
		p.mutex.RLock()
		entry, exists := p.authCache[cacheKey]
		p.mutex.RUnlock()

		if exists && time.Now().Before(entry.expiration) {
			if !entry.result {
				return fmt.Errorf("authentication failed (cached)")
			}
			return nil
		}
	}

	// Prepare request
	authReq := AuthRequest{
		ClientID:  clientID,
		Username:  username,
		Password:  password,
		IPAddress: ipAddress,
	}

	jsonData, err := json.Marshal(authReq)
	if err != nil {
		return fmt.Errorf("error marshaling auth request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", p.config.AuthEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range p.config.Headers {
		req.Header.Set(k, v)
	}

	// Send request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("auth request failed: %w", err)
	}
	defer resp.Body.Close()

	// Handle non-200 status codes
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("auth server returned status %d", resp.StatusCode)
	}

	// Parse response
	var authResp AuthResponse
	err = json.NewDecoder(resp.Body).Decode(&authResp)
	if err != nil {
		return fmt.Errorf("error parsing auth response: %w", err)
	}

	// Cache the result if enabled
	if p.config.EnableAuthCache && p.config.CacheExpiry > 0 {
		cacheKey := fmt.Sprintf("%s:%s:%s", clientID, username, password)
		p.mutex.Lock()
		p.authCache[cacheKey] = cacheEntry{
			result:     authResp.Authenticated,
			expiration: time.Now().Add(time.Duration(p.config.CacheExpiry) * time.Second),
		}
		p.mutex.Unlock()
	}

	// Return error if authentication failed
	if !authResp.Authenticated {
		if authResp.Error != "" {
			return fmt.Errorf("authentication failed: %s", authResp.Error)
		}
		return fmt.Errorf("authentication failed")
	}

	return nil
}

// handleACLCheck handles access control check events
func (p *HTTPAuthPlugin) handleACLCheck(ctx *plugin.Context) error {
	if !p.active || p.config.ACLEndpoint == "" {
		return nil
	}

	// Extract ACL check parameters
	username := ctx.Username
	clientID := ctx.ClientID
	topic := ctx.Topic
	action, _ := ctx.Properties["action"].(string)

	// Check cache first
	if p.config.EnableACLCache && p.config.CacheExpiry > 0 {
		cacheKey := fmt.Sprintf("%s:%s:%s:%s", clientID, username, topic, action)
		p.mutex.RLock()
		entry, exists := p.aclCache[cacheKey]
		p.mutex.RUnlock()

		if exists && time.Now().Before(entry.expiration) {
			if !entry.result {
				return fmt.Errorf("access denied (cached)")
			}
			return nil
		}
	}

	// Prepare request
	aclReq := ACLRequest{
		ClientID: clientID,
		Username: username,
		Topic:    topic,
		Action:   action,
	}

	jsonData, err := json.Marshal(aclReq)
	if err != nil {
		return fmt.Errorf("error marshaling ACL request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", p.config.ACLEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range p.config.Headers {
		req.Header.Set(k, v)
	}

	// Send request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ACL request failed: %w", err)
	}
	defer resp.Body.Close()

	// Handle non-200 status codes
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ACL server returned status %d", resp.StatusCode)
	}

	// Parse response
	var aclResp ACLResponse
	err = json.NewDecoder(resp.Body).Decode(&aclResp)
	if err != nil {
		return fmt.Errorf("error parsing ACL response: %w", err)
	}

	// Cache the result if enabled
	if p.config.EnableACLCache && p.config.CacheExpiry > 0 {
		cacheKey := fmt.Sprintf("%s:%s:%s:%s", clientID, username, topic, action)
		p.mutex.Lock()
		p.aclCache[cacheKey] = cacheEntry{
			result:     aclResp.Allowed,
			expiration: time.Now().Add(time.Duration(p.config.CacheExpiry) * time.Second),
		}
		p.mutex.Unlock()
	}

	// Return error if access is denied
	if !aclResp.Allowed {
		if aclResp.Error != "" {
			return fmt.Errorf("access denied: %s", aclResp.Error)
		}
		return fmt.Errorf("access denied")
	}

	return nil
}

// cleanupCache periodically cleans up expired cache entries
func (p *HTTPAuthPlugin) cleanupCache() {
	for p.active {
		time.Sleep(time.Duration(p.config.CacheExpiry) * time.Second)

		p.mutex.Lock()
		// Clean up auth cache
		for key, entry := range p.authCache {
			if time.Now().After(entry.expiration) {
				delete(p.authCache, key)
			}
		}

		// Clean up ACL cache
		for key, entry := range p.aclCache {
			if time.Now().After(entry.expiration) {
				delete(p.aclCache, key)
			}
		}
		p.mutex.Unlock()
	}
}

// Shutdown stops the plugin
func (p *HTTPAuthPlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()
	log.Printf("HTTP Auth plugin shut down")
	return nil
}

// New creates a new HTTP auth plugin instance
func New() any {
	return NewHTTPAuthPlugin()
}
