package dos_protection

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// DoSConfig represents the DoS protection plugin configuration
type DoSConfig struct {
	// Basic rate limits (per window)
	ConnectionRate  int      `json:"connection_rate"`  // Max connection attempts per IP
	ConnectionBurst int      `json:"connection_burst"` // Max burst of connections
	PublishRate     int      `json:"publish_rate"`     // Max publish events per client
	SubscribeRate   int      `json:"subscribe_rate"`   // Max subscribe events per client
	ByteRate        int      `json:"byte_rate"`        // Max bytes per client
	WindowSize      int      `json:"window_size"`      // Time window in seconds
	IPWhitelist     []string `json:"ip_whitelist"`     // IPs exempt from rate limiting

	// Advanced DoS protection settings
	MaxConnectionsPerIP     int           `json:"max_connections_per_ip"`    // Max concurrent connections per IP
	TemporaryBanDuration    time.Duration `json:"temporary_ban_duration"`    // How long to ban IPs/clients
	FailedAuthThreshold     int           `json:"failed_auth_threshold"`     // Failed auth attempts before ban
	ConnectionFloodInterval time.Duration `json:"connection_flood_interval"` // Time window for connection flood detection
	ConnectionFloodCount    int           `json:"connection_flood_count"`    // Number of connections considered a flood
	GlobalConnectionRate    int           `json:"global_connection_rate"`    // Global rate limit for all connections
	ProgressiveBanEnabled   bool          `json:"progressive_ban_enabled"`   // Enable progressive banning
	MaxBanDuration          time.Duration `json:"max_ban_duration"`          // Maximum ban duration
	EnableLogging           bool          `json:"enable_logging"`            // Enable DoS protection logging
}

// ClientTracker tracks client behavior for DoS protection
type ClientTracker struct {
	connectionCount    int                 // Number of current connections
	failedAuthAttempts int                 // Number of failed authentication attempts
	lastActivity       time.Time           // Last activity time
	banExpiry          time.Time           // When the ban expires
	connections        map[string]struct{} // Track unique connections per client
	violationCount     int                 // Number of rate violations
	temporaryBanCount  int                 // Number of times client has been banned
	mutex              sync.Mutex          // Mutex for thread safety
}

// IPTracker tracks IP address behavior
type IPTracker struct {
	connectionCount    int                 // Number of current connections
	connectionAttempts int                 // Connection attempts in current window
	lastConnectionTime time.Time           // Last connection time
	recentConnections  []time.Time         // Recent connections for flood detection
	clientIDs          map[string]struct{} // Client IDs associated with this IP
	banExpiry          time.Time           // When the ban expires
	violationCount     int                 // Number of violations
	temporaryBanCount  int                 // Number of times IP has been banned
	mutex              sync.Mutex          // Mutex for thread safety
}

// DoSProtectionPlugin is a plugin for advanced DoS protection
type DoSProtectionPlugin struct {
	*plugin.BasePlugin
	config                  *DoSConfig
	ipTrackers              map[string]*IPTracker     // Track IPs
	clientTrackers          map[string]*ClientTracker // Track clients
	bannedIPs               map[string]time.Time      // Banned IPs and expiry times
	bannedClients           map[string]time.Time      // Banned clients and expiry times
	ipWhitelist             []*net.IPNet              // IP whitelist
	globalConnections       int                       // Total current connections
	startTime               time.Time                 // Plugin start time
	cleanupTicker           *time.Ticker              // Ticker for cleanup routine
	metricsMutex            sync.RWMutex              // Mutex for metrics
	connectionRateMutex     sync.Mutex                // Mutex for connection rate tracking
	recentGlobalConnections []time.Time               // Track global connection times
	mutex                   sync.RWMutex              // General mutex
	active                  bool                      // Is plugin active
}

// NewDoSProtectionPlugin creates a new DoS protection plugin
func NewDoSProtectionPlugin() *DoSProtectionPlugin {
	p := &DoSProtectionPlugin{
		BasePlugin:     plugin.NewBasePlugin("dos_protection", "Advanced DoS Protection", "1.0.0", "GoMQTT Team"),
		ipTrackers:     make(map[string]*IPTracker),
		clientTrackers: make(map[string]*ClientTracker),
		bannedIPs:      make(map[string]time.Time),
		bannedClients:  make(map[string]time.Time),
		startTime:      time.Now(),
		active:         true,
	}

	// Register event handlers
	p.RegisterEventHandler(plugin.EventClientConnect, p.handleClientConnect)
	p.RegisterEventHandler(plugin.EventClientDisconnect, p.handleClientDisconnect)
	p.RegisterEventHandler(plugin.EventMessagePublish, p.handleMessagePublish)
	p.RegisterEventHandler(plugin.EventSubscribe, p.handleSubscribe)

	return p
}

// Initialize initializes the DoS protection plugin
func (p *DoSProtectionPlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*DoSConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type for DoS protection plugin")
	}

	p.config = config

	// Set default values if not specified
	if p.config.WindowSize <= 0 {
		p.config.WindowSize = 60 // Default 60 seconds
	}
	if p.config.TemporaryBanDuration <= 0 {
		p.config.TemporaryBanDuration = 5 * time.Minute // Default 5 minutes
	}
	if p.config.ConnectionFloodInterval <= 0 {
		p.config.ConnectionFloodInterval = 10 * time.Second // Default 10 seconds
	}
	if p.config.MaxBanDuration <= 0 {
		p.config.MaxBanDuration = 24 * time.Hour // Default 24 hours
	}

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

	// Start cleanup routine
	p.cleanupTicker = time.NewTicker(time.Duration(p.config.WindowSize) * time.Second)
	go p.startCleanupRoutine()

	log.Printf("DoS protection plugin initialized with connection_rate=%d, global_rate=%d",
		config.ConnectionRate, config.GlobalConnectionRate)
	return nil
}

// isWhitelisted checks if an IP is in the whitelist
func (p *DoSProtectionPlugin) isWhitelisted(ipStr string) bool {
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

// isIPBanned checks if an IP is currently banned
func (p *DoSProtectionPlugin) isIPBanned(ipStr string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	expiryTime, banned := p.bannedIPs[ipStr]
	return banned && time.Now().Before(expiryTime)
}

// isClientBanned checks if a client is currently banned
func (p *DoSProtectionPlugin) isClientBanned(clientID string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	expiryTime, banned := p.bannedClients[clientID]
	return banned && time.Now().Before(expiryTime)
}

// banIP temporarily bans an IP address
func (p *DoSProtectionPlugin) banIP(ipStr string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Get existing tracker or create a new one
	tracker, exists := p.ipTrackers[ipStr]
	if !exists {
		tracker = &IPTracker{
			lastConnectionTime: time.Now(),
			clientIDs:          make(map[string]struct{}),
		}
		p.ipTrackers[ipStr] = tracker
	}

	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	// Calculate ban duration with progressive penalties
	banDuration := p.config.TemporaryBanDuration
	if p.config.ProgressiveBanEnabled && tracker.temporaryBanCount > 0 {
		// Double ban duration for each previous ban, up to max
		multiplier := 1 << uint(tracker.temporaryBanCount) // 2^temporaryBanCount
		banDuration = p.config.TemporaryBanDuration * time.Duration(multiplier)

		// Cap at maximum ban duration
		if banDuration > p.config.MaxBanDuration {
			banDuration = p.config.MaxBanDuration
		}
	}

	// Record ban
	p.bannedIPs[ipStr] = time.Now().Add(banDuration)
	tracker.temporaryBanCount++

	if p.config.EnableLogging {
		log.Printf("IP %s banned for %v due to DoS protection violation (violation #%d)",
			ipStr, banDuration, tracker.temporaryBanCount)
	}
}

// banClient temporarily bans a client
func (p *DoSProtectionPlugin) banClient(clientID string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Get existing tracker or create a new one
	tracker, exists := p.clientTrackers[clientID]
	if !exists {
		tracker = &ClientTracker{
			lastActivity: time.Now(),
			connections:  make(map[string]struct{}),
		}
		p.clientTrackers[clientID] = tracker
	}

	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	// Calculate ban duration with progressive penalties
	banDuration := p.config.TemporaryBanDuration
	if p.config.ProgressiveBanEnabled && tracker.temporaryBanCount > 0 {
		// Double ban duration for each previous ban, up to max
		multiplier := 1 << uint(tracker.temporaryBanCount) // 2^temporaryBanCount
		banDuration = p.config.TemporaryBanDuration * time.Duration(multiplier)

		// Cap at maximum ban duration
		if banDuration > p.config.MaxBanDuration {
			banDuration = p.config.MaxBanDuration
		}
	}

	// Record ban
	p.bannedClients[clientID] = time.Now().Add(banDuration)
	tracker.temporaryBanCount++

	if p.config.EnableLogging {
		log.Printf("Client %s banned for %v due to DoS protection violation (violation #%d)",
			clientID, banDuration, tracker.temporaryBanCount)
	}
}

// checkGlobalConnectionRate checks if global connection rate is exceeded
func (p *DoSProtectionPlugin) checkGlobalConnectionRate() bool {
	p.connectionRateMutex.Lock()
	defer p.connectionRateMutex.Unlock()

	now := time.Now()

	// Remove connections outside the flood interval window
	cutoff := now.Add(-p.config.ConnectionFloodInterval)
	newConnections := []time.Time{}

	for _, t := range p.recentGlobalConnections {
		if t.After(cutoff) {
			newConnections = append(newConnections, t)
		}
	}

	// Add current connection time
	newConnections = append(newConnections, now)
	p.recentGlobalConnections = newConnections

	// Check if we're over the global rate limit
	return p.config.GlobalConnectionRate > 0 && len(p.recentGlobalConnections) > p.config.GlobalConnectionRate
}

// checkConnectionFlood checks if an IP is flooding with connections
func (p *DoSProtectionPlugin) checkConnectionFlood(ipStr string) bool {
	tracker, exists := p.ipTrackers[ipStr]
	if !exists {
		return false
	}

	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	now := time.Now()

	// Remove connections outside the flood interval window
	cutoff := now.Add(-p.config.ConnectionFloodInterval)
	newConnections := []time.Time{}

	for _, t := range tracker.recentConnections {
		if t.After(cutoff) {
			newConnections = append(newConnections, t)
		}
	}

	// Add current connection time
	newConnections = append(newConnections, now)
	tracker.recentConnections = newConnections

	// Check if connection count exceeds flood threshold
	return p.config.ConnectionFloodCount > 0 && len(newConnections) > p.config.ConnectionFloodCount
}

// handleClientConnect handles client connect events with DoS protection
func (p *DoSProtectionPlugin) handleClientConnect(ctx *plugin.Context) error {
	if !p.active {
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

	// Check if IP is banned
	if p.isIPBanned(ipStr) {
		return fmt.Errorf("connection rejected: IP %s is temporarily banned", ipStr)
	}

	// Check if client is banned
	if ctx.ClientID != "" && p.isClientBanned(ctx.ClientID) {
		return fmt.Errorf("connection rejected: client %s is temporarily banned", ctx.ClientID)
	}

	// Check global connection rate
	if p.checkGlobalConnectionRate() {
		if p.config.EnableLogging {
			log.Printf("Global connection rate exceeded, rejecting connection from %s", ipStr)
		}
		return fmt.Errorf("connection rejected: global connection rate exceeded")
	}

	// Update IP tracking
	p.mutex.Lock()
	tracker, exists := p.ipTrackers[ipStr]
	if !exists {
		tracker = &IPTracker{
			lastConnectionTime: time.Now(),
			clientIDs:          make(map[string]struct{}),
		}
		p.ipTrackers[ipStr] = tracker
	}
	p.mutex.Unlock()

	tracker.mutex.Lock()

	// Check for connection flooding
	if p.checkConnectionFlood(ipStr) {
		tracker.mutex.Unlock()
		p.banIP(ipStr)
		return fmt.Errorf("connection rejected: connection flood detected from IP %s", ipStr)
	}

	// Check connection rate per window
	windowSize := p.config.WindowSize
	if windowSize <= 0 {
		windowSize = 60 // Default to 60 seconds
	}

	// Reset counter if window has elapsed
	if time.Since(tracker.lastConnectionTime).Seconds() > float64(windowSize) {
		tracker.connectionAttempts = 0
		tracker.lastConnectionTime = time.Now()
	}

	// Increment and check connection attempt counter
	tracker.connectionAttempts++

	// Check connection rate
	if p.config.ConnectionRate > 0 && tracker.connectionAttempts > p.config.ConnectionRate {
		tracker.violationCount++

		// If configured, automatically ban after multiple violations
		if tracker.violationCount >= 3 {
			tracker.mutex.Unlock()
			p.banIP(ipStr)
			return fmt.Errorf("connection rejected: connection rate exceeded for IP %s", ipStr)
		}

		tracker.mutex.Unlock()
		return fmt.Errorf("connection rate exceeded for IP %s", ipStr)
	}

	// Check max connections per IP
	if p.config.MaxConnectionsPerIP > 0 && tracker.connectionCount >= p.config.MaxConnectionsPerIP {
		tracker.violationCount++
		tracker.mutex.Unlock()
		return fmt.Errorf("maximum connections per IP exceeded for %s", ipStr)
	}

	// Update connection count
	tracker.connectionCount++

	// Associate client ID with IP
	if ctx.ClientID != "" {
		tracker.clientIDs[ctx.ClientID] = struct{}{}
	}
	tracker.mutex.Unlock()

	// Update client tracking
	if ctx.ClientID != "" {
		p.mutex.Lock()
		clientTracker, exists := p.clientTrackers[ctx.ClientID]
		if !exists {
			clientTracker = &ClientTracker{
				lastActivity: time.Now(),
				connections:  make(map[string]struct{}),
			}
			p.clientTrackers[ctx.ClientID] = clientTracker
		}
		p.mutex.Unlock()

		clientTracker.mutex.Lock()
		clientTracker.connectionCount++
		// Store unique connection identifier (could use session ID or similar)
		connID := fmt.Sprintf("%s-%d", ipStr, time.Now().UnixNano())
		clientTracker.connections[connID] = struct{}{}
		clientTracker.mutex.Unlock()
	}

	// Update global connection count
	p.metricsMutex.Lock()
	p.globalConnections++
	p.metricsMutex.Unlock()

	return nil
}

// handleClientDisconnect handles client disconnect events
func (p *DoSProtectionPlugin) handleClientDisconnect(ctx *plugin.Context) error {
	if !p.active {
		return nil
	}

	// Update client tracking
	if ctx.ClientID != "" {
		p.mutex.RLock()
		clientTracker, exists := p.clientTrackers[ctx.ClientID]
		p.mutex.RUnlock()

		if exists {
			clientTracker.mutex.Lock()
			if clientTracker.connectionCount > 0 {
				clientTracker.connectionCount--
			}
			clientTracker.lastActivity = time.Now()
			clientTracker.mutex.Unlock()
		}
	}

	// Update IP connection count if available
	ipStr, ok := ctx.Properties["ip"].(string)
	if ok {
		p.mutex.RLock()
		ipTracker, exists := p.ipTrackers[ipStr]
		p.mutex.RUnlock()

		if exists {
			ipTracker.mutex.Lock()
			if ipTracker.connectionCount > 0 {
				ipTracker.connectionCount--
			}
			ipTracker.mutex.Unlock()
		}
	}

	// Update global connection count
	p.metricsMutex.Lock()
	if p.globalConnections > 0 {
		p.globalConnections--
	}
	p.metricsMutex.Unlock()

	return nil
}

// handleMessagePublish handles message publish events with DoS protection
func (p *DoSProtectionPlugin) handleMessagePublish(ctx *plugin.Context) error {
	if !p.active || p.config.PublishRate <= 0 {
		return nil
	}

	// Skip if client ID is not set
	if ctx.ClientID == "" {
		return nil
	}

	// Check if client is banned
	if p.isClientBanned(ctx.ClientID) {
		return fmt.Errorf("publish rejected: client %s is temporarily banned", ctx.ClientID)
	}

	// Get or create client tracker
	p.mutex.Lock()
	tracker, exists := p.clientTrackers[ctx.ClientID]
	if !exists {
		tracker = &ClientTracker{
			lastActivity: time.Now(),
			connections:  make(map[string]struct{}),
		}
		p.clientTrackers[ctx.ClientID] = tracker
	}
	p.mutex.Unlock()

	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	// Check message rate
	windowSize := p.config.WindowSize
	if windowSize <= 0 {
		windowSize = 60
	}

	// Count as violation and potentially ban after multiple violations
	if ctx.Properties != nil {
		if msgRate, ok := ctx.Properties["message_rate"].(int); ok && msgRate > p.config.PublishRate {
			tracker.violationCount++

			if tracker.violationCount >= 3 {
				tracker.mutex.Unlock() // Unlock before ban to prevent deadlock
				p.banClient(ctx.ClientID)
				return fmt.Errorf("publish rate exceeded for client %s", ctx.ClientID)
			}

			return fmt.Errorf("publish rate warning for client %s", ctx.ClientID)
		}
	}

	// Check byte rate if applicable
	if p.config.ByteRate > 0 && ctx.Payload != nil {
		if ctx.Properties != nil {
			if byteRate, ok := ctx.Properties["byte_rate"].(int); ok && byteRate > p.config.ByteRate {
				tracker.violationCount++

				if tracker.violationCount >= 3 {
					tracker.mutex.Unlock() // Unlock before ban to prevent deadlock
					p.banClient(ctx.ClientID)
					return fmt.Errorf("byte rate exceeded for client %s", ctx.ClientID)
				}

				return fmt.Errorf("byte rate warning for client %s", ctx.ClientID)
			}
		}
	}

	tracker.lastActivity = time.Now()
	return nil
}

// handleSubscribe handles subscribe events with DoS protection
func (p *DoSProtectionPlugin) handleSubscribe(ctx *plugin.Context) error {
	if !p.active || p.config.SubscribeRate <= 0 {
		return nil
	}

	// Skip if client ID is not set
	if ctx.ClientID == "" {
		return nil
	}

	// Check if client is banned
	if p.isClientBanned(ctx.ClientID) {
		return fmt.Errorf("subscribe rejected: client %s is temporarily banned", ctx.ClientID)
	}

	// Get or create client tracker
	p.mutex.Lock()
	tracker, exists := p.clientTrackers[ctx.ClientID]
	if !exists {
		tracker = &ClientTracker{
			lastActivity: time.Now(),
			connections:  make(map[string]struct{}),
		}
		p.clientTrackers[ctx.ClientID] = tracker
	}
	p.mutex.Unlock()

	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	// Check subscription rate
	windowSize := p.config.WindowSize
	if windowSize <= 0 {
		windowSize = 60
	}

	// Count as violation and potentially ban after multiple violations
	if ctx.Properties != nil {
		if subRate, ok := ctx.Properties["subscribe_rate"].(int); ok && subRate > p.config.SubscribeRate {
			tracker.violationCount++

			if tracker.violationCount >= 3 {
				tracker.mutex.Unlock() // Unlock before ban to prevent deadlock
				p.banClient(ctx.ClientID)
				return fmt.Errorf("subscribe rate exceeded for client %s", ctx.ClientID)
			}

			return fmt.Errorf("subscribe rate warning for client %s", ctx.ClientID)
		}
	}

	tracker.lastActivity = time.Now()
	return nil
}

// startCleanupRoutine starts a goroutine to periodically clean up trackers and expired bans
func (p *DoSProtectionPlugin) startCleanupRoutine() {
	for range p.cleanupTicker.C {
		if !p.active {
			break
		}

		p.cleanupExpiredBans()
		p.cleanupInactiveTrackers()
	}
}

// cleanupExpiredBans removes expired bans
func (p *DoSProtectionPlugin) cleanupExpiredBans() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()

	// Clean up expired IP bans
	for ip, expiry := range p.bannedIPs {
		if now.After(expiry) {
			delete(p.bannedIPs, ip)
			if p.config.EnableLogging {
				log.Printf("Ban expired for IP %s", ip)
			}
		}
	}

	// Clean up expired client bans
	for clientID, expiry := range p.bannedClients {
		if now.After(expiry) {
			delete(p.bannedClients, clientID)
			if p.config.EnableLogging {
				log.Printf("Ban expired for client %s", clientID)
			}
		}
	}
}

// cleanupInactiveTrackers removes inactive trackers to prevent memory leaks
func (p *DoSProtectionPlugin) cleanupInactiveTrackers() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()
	inactivityThreshold := time.Duration(p.config.WindowSize*2) * time.Second

	// Clean up inactive IP trackers
	for ip, tracker := range p.ipTrackers {
		tracker.mutex.Lock()
		inactive := tracker.connectionCount == 0 && now.Sub(tracker.lastConnectionTime) > inactivityThreshold
		tracker.mutex.Unlock()

		if inactive {
			delete(p.ipTrackers, ip)
		}
	}

	// Clean up inactive client trackers
	for clientID, tracker := range p.clientTrackers {
		tracker.mutex.Lock()
		inactive := tracker.connectionCount == 0 && now.Sub(tracker.lastActivity) > inactivityThreshold
		tracker.mutex.Unlock()

		if inactive {
			delete(p.clientTrackers, clientID)
		}
	}
}

// Shutdown stops the DoS protection plugin
func (p *DoSProtectionPlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()

	if p.cleanupTicker != nil {
		p.cleanupTicker.Stop()
	}

	log.Printf("DoS protection plugin shut down")
	return nil
}

// New creates a new DoS protection plugin instance
func New() any {
	return NewDoSProtectionPlugin()
}
