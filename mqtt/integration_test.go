package mqtt

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// mockNetwork simulates a network with multiple clients for integration testing
type mockNetwork struct {
	server     *Server
	clients    map[string]*mockNetConn
	clientsMu  sync.Mutex
	messageLog []string
}

// mockNetConn is a simulated network connection
type mockNetConn struct {
	id         string
	network    *mockNetwork
	remoteConn *mockNetConn
	readCh     chan []byte
	closed     bool
	localAddr  net.Addr
	remoteAddr net.Addr
}

func newMockNetwork() *mockNetwork {
	return &mockNetwork{
		server:  NewServer("localhost", 1883),
		clients: make(map[string]*mockNetConn),
	}
}

func (mn *mockNetwork) createClient(id string) *mockNetConn {
	clientConn, serverConn := newMockNetConnPair(id, mn)

	mn.clientsMu.Lock()
	mn.clients[id] = clientConn
	mn.clientsMu.Unlock()

	// Process this connection in a new goroutine
	go mn.server.handleConnection(serverConn)

	return clientConn
}

func (mn *mockNetwork) logMessage(msg string) {
	mn.messageLog = append(mn.messageLog, msg)
}

func newMockNetConnPair(id string, network *mockNetwork) (*mockNetConn, *mockNetConn) {
	client := &mockNetConn{
		id:         id + "_client",
		network:    network,
		readCh:     make(chan []byte, 10),
		localAddr:  &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883},
	}

	server := &mockNetConn{
		id:         id + "_server",
		network:    network,
		readCh:     make(chan []byte, 10),
		localAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345},
	}

	client.remoteConn = server
	server.remoteConn = client

	return client, server
}

func (c *mockNetConn) Read(b []byte) (n int, err error) {
	if c.closed {
		return 0, net.ErrClosed
	}

	select {
	case data := <-c.readCh:
		n = copy(b, data)
		return n, nil
	case <-time.After(100 * time.Millisecond):
		return 0, nil
	}
}

func (c *mockNetConn) Write(b []byte) (n int, err error) {
	if c.closed {
		return 0, net.ErrClosed
	}

	data := make([]byte, len(b))
	copy(data, b)

	// Write to the remote connection's read channel
	c.remoteConn.readCh <- data

	return len(b), nil
}

func (c *mockNetConn) Close() error {
	c.closed = true
	return nil
}

func (c *mockNetConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *mockNetConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *mockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// Helper function to connect a client
func connectClient(conn net.Conn, clientID string, cleanSession bool) error {
	// Create connect packet
	packet := NewConnectPacket(clientID, "", nil, cleanSession)

	// Encode and send
	data, err := packet.Encode()
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	if err != nil {
		return err
	}

	// Wait for CONNACK
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	// Parse the response
	reader := bytes.NewReader(buf[:n])
	resp, err := ReadPacket(reader)
	if err != nil {
		return err
	}

	// Verify it's a CONNACK
	if resp.PacketType != CONNACK {
		return fmt.Errorf("expected CONNACK, got %d", resp.PacketType)
	}

	// Verify successful connection
	if resp.ReturnCode != ConnAccepted {
		return fmt.Errorf("connection failed with code %d", resp.ReturnCode)
	}

	return nil
}

// Helper function to subscribe a client to a topic
func subscribeTopic(conn net.Conn, topic string, qos byte) error {
	// Create subscribe packet
	packet := NewSubscribePacket([]string{topic}, []byte{qos}, 1)

	// Encode and send
	data, err := packet.Encode()
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	if err != nil {
		return err
	}

	// Wait for SUBACK
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	// Parse the response
	reader := bytes.NewReader(buf[:n])
	resp, err := ReadPacket(reader)
	if err != nil {
		return err
	}

	// Verify it's a SUBACK
	if resp.PacketType != SUBACK {
		return fmt.Errorf("expected SUBACK, got %d", resp.PacketType)
	}

	return nil
}

// Helper function to publish a message
func publishMessage(conn net.Conn, topic string, payload []byte, qos byte, retain bool) error {
	// Create publish packet
	packet := NewPublishPacket(topic, payload, qos, 1, false, retain)

	// Encode and send
	data, err := packet.Encode()
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}

// Helper function to read a published message
func readPublishedMessage(conn net.Conn, timeout time.Duration) (*Packet, error) {
	// Set a deadline if timeout is provided
	if timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(timeout))
		defer conn.SetReadDeadline(time.Time{})
	}

	// Read response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	// Parse the packet
	reader := bytes.NewReader(buf[:n])
	packet, err := ReadPacket(reader)
	if err != nil {
		return nil, err
	}

	return packet, nil
}

func TestIntegrationBasicPubSub(t *testing.T) {
	// Create mock network with broker
	network := newMockNetwork()

	// Create publisher client
	pubConn := network.createClient("publisher")
	err := connectClient(pubConn, "publisher", true)
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}

	// Create subscriber client
	subConn := network.createClient("subscriber")
	err = connectClient(subConn, "subscriber", true)
	if err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}

	// Subscribe to topic
	err = subscribeTopic(subConn, "test/topic", 0)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait a bit for subscription to be processed
	time.Sleep(100 * time.Millisecond)

	// Publish message
	err = publishMessage(pubConn, "test/topic", []byte("Hello, MQTT!"), 0, false)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Read published message
	packet, err := readPublishedMessage(subConn, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to read published message: %v", err)
	}

	// Verify message
	if packet.PacketType != PUBLISH {
		t.Errorf("Expected PUBLISH packet, got %d", packet.PacketType)
	}
	if packet.TopicName != "test/topic" {
		t.Errorf("Expected topic 'test/topic', got '%s'", packet.TopicName)
	}
	if string(packet.Payload) != "Hello, MQTT!" {
		t.Errorf("Expected payload 'Hello, MQTT!', got '%s'", string(packet.Payload))
	}
}

func TestIntegrationRetainedMessages(t *testing.T) {
	// Create mock network with broker
	network := newMockNetwork()

	// Create publisher client
	pubConn := network.createClient("publisher")
	err := connectClient(pubConn, "publisher", true)
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}

	// Publish retained message
	err = publishMessage(pubConn, "test/retained", []byte("Retained message"), 0, true)
	if err != nil {
		t.Fatalf("Failed to publish retained message: %v", err)
	}

	// Wait a bit for retained message to be processed
	time.Sleep(100 * time.Millisecond)

	// Create subscriber client
	subConn := network.createClient("subscriber")
	err = connectClient(subConn, "subscriber", true)
	if err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}

	// Subscribe to topic (should receive retained message immediately)
	err = subscribeTopic(subConn, "test/retained", 0)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Read retained message
	packet, err := readPublishedMessage(subConn, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to read retained message: %v", err)
	}

	// Verify message
	if packet.PacketType != PUBLISH {
		t.Errorf("Expected PUBLISH packet, got %d", packet.PacketType)
	}
	if packet.TopicName != "test/retained" {
		t.Errorf("Expected topic 'test/retained', got '%s'", packet.TopicName)
	}
	if string(packet.Payload) != "Retained message" {
		t.Errorf("Expected payload 'Retained message', got '%s'", string(packet.Payload))
	}
	if !packet.Retain {
		t.Error("Expected retained flag to be set")
	}
}

func TestIntegrationSharedSubscription(t *testing.T) {
	// Create mock network with broker
	network := newMockNetwork()

	// Create publisher client
	pubConn := network.createClient("publisher")
	err := connectClient(pubConn, "publisher", true)
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}

	// Create subscriber 1
	sub1Conn := network.createClient("subscriber1")
	err = connectClient(sub1Conn, "subscriber1", true)
	if err != nil {
		t.Fatalf("Failed to connect subscriber1: %v", err)
	}

	// Create subscriber 2
	sub2Conn := network.createClient("subscriber2")
	err = connectClient(sub2Conn, "subscriber2", true)
	if err != nil {
		t.Fatalf("Failed to connect subscriber2: %v", err)
	}

	// Subscribe both clients to the same shared subscription
	err = subscribeTopic(sub1Conn, "$share/group1/test/shared", 0)
	if err != nil {
		t.Fatalf("Failed to subscribe subscriber1: %v", err)
	}

	err = subscribeTopic(sub2Conn, "$share/group1/test/shared", 0)
	if err != nil {
		t.Fatalf("Failed to subscribe subscriber2: %v", err)
	}

	// Wait a bit for subscriptions to be processed
	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages
	for i := 0; i < 10; i++ {
		err = publishMessage(pubConn, "test/shared", []byte(fmt.Sprintf("Message %d", i)), 0, false)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Each subscriber should receive some messages, but not all
	messages1 := 0
	messages2 := 0

	// Check for messages on subscriber 1
	for i := 0; i < 10; i++ {
		packet, err := readPublishedMessage(sub1Conn, 50*time.Millisecond)
		if err != nil {
			// No more messages for this subscriber
			break
		}
		if packet != nil {
			messages1++
		}
	}

	// Check for messages on subscriber 2
	for i := 0; i < 10; i++ {
		packet, err := readPublishedMessage(sub2Conn, 50*time.Millisecond)
		if err != nil {
			// No more messages for this subscriber
			break
		}
		if packet != nil {
			messages2++
		}
	}

	// Verify each subscriber got some messages, but not all
	if messages1 == 0 {
		t.Error("Expected subscriber1 to receive some messages, got none")
	}
	if messages2 == 0 {
		t.Error("Expected subscriber2 to receive some messages, got none")
	}
	if messages1+messages2 != 10 {
		t.Errorf("Expected 10 total messages, got %d", messages1+messages2)
	}
}
