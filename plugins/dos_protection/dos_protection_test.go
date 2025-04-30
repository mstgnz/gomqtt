package dos_protection

import (
	"testing"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

func TestDoSProtectionPlugin(t *testing.T) {
	// Create plugin
	p := NewDoSProtectionPlugin()

	// Create test config
	config := &DoSConfig{
		ConnectionRate:          10,
		PublishRate:             50,
		SubscribeRate:           20,
		ByteRate:                1024 * 1024, // 1MB
		WindowSize:              60,
		IPWhitelist:             []string{"127.0.0.1", "192.168.0.0/24"},
		MaxConnectionsPerIP:     5,
		TemporaryBanDuration:    2 * time.Minute,
		FailedAuthThreshold:     3,
		ConnectionFloodInterval: 5 * time.Second,
		ConnectionFloodCount:    20,
		GlobalConnectionRate:    100,
		ProgressiveBanEnabled:   true,
		MaxBanDuration:          time.Hour,
		EnableLogging:           false,
	}

	// Initialize plugin
	err := p.Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize plugin: %v", err)
	}

	// Test whitelist functionality
	if !p.isWhitelisted("127.0.0.1") {
		t.Error("Expected 127.0.0.1 to be whitelisted")
	}

	if !p.isWhitelisted("192.168.0.10") {
		t.Error("Expected 192.168.0.10 to be whitelisted")
	}

	if p.isWhitelisted("10.0.0.1") {
		t.Error("Expected 10.0.0.1 to not be whitelisted")
	}

	// Test IP banning
	p.banIP("10.0.0.1")
	if !p.isIPBanned("10.0.0.1") {
		t.Error("Expected 10.0.0.1 to be banned")
	}

	// Test client banning
	p.banClient("test-client")
	if !p.isClientBanned("test-client") {
		t.Error("Expected test-client to be banned")
	}

	// Test client connection handling
	ctx := &plugin.Context{
		Event:      plugin.EventClientConnect,
		ClientID:   "test-client2",
		Properties: map[string]any{"ip": "8.8.8.8"},
	}

	// First connection from this IP should be allowed
	err = p.handleClientConnect(ctx)
	if err != nil {
		t.Errorf("First connection should be allowed, got error: %v", err)
	}

	// Test that banned client is rejected
	ctx.ClientID = "test-client"
	err = p.handleClientConnect(ctx)
	if err == nil {
		t.Error("Expected banned client to be rejected")
	}

	// Test client disconnection
	ctx.ClientID = "test-client2"
	err = p.handleClientDisconnect(ctx)
	if err != nil {
		t.Errorf("Client disconnect failed: %v", err)
	}

	// Test cleanup of expired bans (by directly manipulating the ban expiry)
	p.mutex.Lock()
	p.bannedIPs["10.0.0.2"] = time.Now().Add(-time.Minute)           // Already expired
	p.bannedClients["expired-client"] = time.Now().Add(-time.Minute) // Already expired
	p.mutex.Unlock()

	// Run cleanup
	p.cleanupExpiredBans()

	// Check if expired bans were cleaned up
	if p.isIPBanned("10.0.0.2") {
		t.Error("Expected expired IP ban to be cleaned up")
	}

	if p.isClientBanned("expired-client") {
		t.Error("Expected expired client ban to be cleaned up")
	}

	// Test progressive banning
	p.banIP("10.0.0.3")
	p.banIP("10.0.0.3") // Second ban should be longer

	// Test tracker cleanup
	p.mutex.Lock()
	// Add an inactive tracker that should be cleaned up
	p.ipTrackers["10.0.0.99"] = &IPTracker{
		lastConnectionTime: time.Now().Add(-time.Hour),
		connectionCount:    0,
		clientIDs:          make(map[string]struct{}),
	}
	p.clientTrackers["inactive-client"] = &ClientTracker{
		lastActivity:    time.Now().Add(-time.Hour),
		connectionCount: 0,
		connections:     make(map[string]struct{}),
	}
	p.mutex.Unlock()

	// Run cleanup
	p.cleanupInactiveTrackers()

	// Check that inactive trackers were cleaned up
	p.mutex.RLock()
	_, ipExists := p.ipTrackers["10.0.0.99"]
	_, clientExists := p.clientTrackers["inactive-client"]
	p.mutex.RUnlock()

	if ipExists {
		t.Error("Expected inactive IP tracker to be cleaned up")
	}

	if clientExists {
		t.Error("Expected inactive client tracker to be cleaned up")
	}

	// Test message publish handling
	pubCtx := &plugin.Context{
		Event:      plugin.EventMessagePublish,
		ClientID:   "publisher-client",
		Properties: map[string]any{"message_rate": 40}, // Below limit
	}

	// This publish should be allowed (below rate limit)
	err = p.handleMessagePublish(pubCtx)
	if err != nil {
		t.Errorf("Expected publish to be allowed, got error: %v", err)
	}

	// Exceed publish rate
	pubCtx.Properties["message_rate"] = 60 // Above limit
	err = p.handleMessagePublish(pubCtx)
	if err == nil {
		t.Error("Expected error for exceeding publish rate")
	}

	// Test subscribe handling
	subCtx := &plugin.Context{
		Event:      plugin.EventSubscribe,
		ClientID:   "subscriber-client",
		Properties: map[string]any{"subscribe_rate": 15}, // Below limit
	}

	// This subscribe should be allowed (below rate limit)
	err = p.handleSubscribe(subCtx)
	if err != nil {
		t.Errorf("Expected subscribe to be allowed, got error: %v", err)
	}

	// Exceed subscribe rate
	subCtx.Properties["subscribe_rate"] = 30 // Above limit
	err = p.handleSubscribe(subCtx)
	if err == nil {
		t.Error("Expected error for exceeding subscribe rate")
	}

	// Test connection flooding detection
	floodCtx := &plugin.Context{
		Event:      plugin.EventClientConnect,
		ClientID:   "flood-client",
		Properties: map[string]any{"ip": "192.168.1.100"}, // Not whitelisted
	}

	// Simulate connection flood by directly adding connections
	p.mutex.Lock()
	ipTracker, exists := p.ipTrackers["192.168.1.100"]
	if !exists {
		ipTracker = &IPTracker{
			lastConnectionTime: time.Now(),
			clientIDs:          make(map[string]struct{}),
			recentConnections:  make([]time.Time, 0),
		}
		p.ipTrackers["192.168.1.100"] = ipTracker
	}

	// Add enough connections to trigger flood detection
	now := time.Now()
	for i := 0; i < config.ConnectionFloodCount+1; i++ {
		ipTracker.recentConnections = append(ipTracker.recentConnections, now)
	}
	p.mutex.Unlock()

	// This should trigger flood detection and ban
	err = p.handleClientConnect(floodCtx)
	if err == nil {
		t.Error("Expected connection to be rejected due to flooding")
	}

	// Clean up
	p.Shutdown()
}

func TestDoSProtectionInitDefaults(t *testing.T) {
	// Test that default values are set correctly
	p := NewDoSProtectionPlugin()

	// Empty config with no values set
	config := &DoSConfig{}

	err := p.Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize plugin: %v", err)
	}

	// Check default values
	if p.config.WindowSize != 60 {
		t.Errorf("Expected default WindowSize=60, got %d", p.config.WindowSize)
	}

	if p.config.TemporaryBanDuration != 5*time.Minute {
		t.Errorf("Expected default TemporaryBanDuration=5min, got %v", p.config.TemporaryBanDuration)
	}

	if p.config.ConnectionFloodInterval != 10*time.Second {
		t.Errorf("Expected default ConnectionFloodInterval=10s, got %v", p.config.ConnectionFloodInterval)
	}

	if p.config.MaxBanDuration != 24*time.Hour {
		t.Errorf("Expected default MaxBanDuration=24h, got %v", p.config.MaxBanDuration)
	}

	// Clean up
	p.Shutdown()
}

func TestInvalidConfiguration(t *testing.T) {
	p := NewDoSProtectionPlugin()

	// Pass invalid config type
	err := p.Initialize("invalid")
	if err == nil {
		t.Error("Expected error for invalid configuration type")
	}
}
