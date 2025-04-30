package bridge

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// CoAPBridgeConfig represents a CoAP bridge configuration
type CoAPBridgeConfig struct {
	Enabled     bool   `json:"enabled"`
	Name        string `json:"name"`
	Address     string `json:"address"`
	TopicFilter string `json:"topic_filter"`
	QoS         byte   `json:"qos"`
	Timeout     int    `json:"timeout"`
}

// CoAPBridge implements a bridge between MQTT and CoAP
type CoAPBridge struct {
	*BridgePlugin
	coapConfig CoAPBridgeConfig
	conn       *net.UDPConn
	active     bool
}

// NewCoAPBridge creates a new CoAP bridge
func NewCoAPBridge(basePlugin *BridgePlugin, config CoAPBridgeConfig) (*CoAPBridge, error) {
	bridge := &CoAPBridge{
		BridgePlugin: basePlugin,
		coapConfig:   config,
		active:       config.Enabled,
	}

	// Setup UDP connection for CoAP (which uses UDP)
	udpAddr, err := net.ResolveUDPAddr("udp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CoAP server address: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial CoAP server: %v", err)
	}

	bridge.conn = conn

	log.Printf("CoAP bridge '%s' initialized with endpoint %s", config.Name, config.Address)
	return bridge, nil
}

// HandleMessage sends a message to the CoAP endpoint
func (b *CoAPBridge) HandleMessage(ctx *plugin.Context) error {
	if !b.active || !b.coapConfig.Enabled {
		return nil
	}

	// Check QoS level
	if ctx.QoS < b.coapConfig.QoS {
		return nil
	}

	// Check if topic matches the filter
	if !isTopicMatch(b.coapConfig.TopicFilter, ctx.Topic) {
		return nil
	}

	// Create a simple CoAP message (this is simplified)
	// In a real implementation, you would create a proper CoAP message format
	// CoAP message format: Version (2 bits) + Type (2 bits) + Token length (4 bits) + Code (8 bits) + Message ID (16 bits) + Token (0-8 bytes) + Options + Payload
	// This is a simplified version just for the demo
	coapMsg := []byte{
		0x40,       // Version 1, Type 0 (Confirmable), Token Length 0
		0x02,       // Code 2 (POST)
		0x00, 0x01, // Message ID
		0xFF, // Payload marker
	}

	// Add the payload
	coapMsg = append(coapMsg, ctx.Payload...)

	// Set write deadline
	timeout := 10
	if b.coapConfig.Timeout > 0 {
		timeout = b.coapConfig.Timeout
	}
	b.conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second))

	// Send the CoAP message
	_, err := b.conn.Write(coapMsg)
	if err != nil {
		log.Printf("CoAP bridge '%s' error sending message: %v", b.coapConfig.Name, err)
		return err
	}

	log.Printf("CoAP bridge '%s' sent message to %s", b.coapConfig.Name, b.coapConfig.Address)
	return nil
}

// Close shuts down the CoAP bridge
func (b *CoAPBridge) Close() error {
	b.active = false
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}
