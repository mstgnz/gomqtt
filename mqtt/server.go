package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mstgnz/gomqtt/rate"
)

// Server represents the MQTT broker server
type Server struct {
	// Configuration
	Host string
	Port int

	// TLS configuration
	TLSEnabled           bool
	TLSPort              int
	TLSCertFile          string
	TLSKeyFile           string
	TLSRequireClientCert bool
	TLSCACertFile        string

	// WebSocket configuration
	WSEnabled bool
	WSHost    string
	WSPort    int
	WSPath    string

	// Secure WebSocket configuration
	WSSTLSEnabled  bool
	WSSTLSPort     int
	WSSTLSCertFile string
	WSSTLSKeyFile  string

	// TCP listener
	listener net.Listener

	// TLS listener
	tlsListener net.Listener

	// WebSocket listener
	wsServer   *http.Server
	wsUpgrader *websocket.Upgrader

	// Secure WebSocket listener
	wssServer *http.Server

	// Client management
	clients      map[string]*Client
	clientsMutex sync.RWMutex

	// Topics and subscription management
	subscriptions map[string][]*Subscription
	subMutex      sync.RWMutex

	// Retained messages
	retainedMessages      map[string]RetainedMessage
	retainedMessagesMutex sync.RWMutex

	// QoS management
	inflightMessages    map[string]map[uint16]*InflightMessage // clientID -> messageID -> message
	inflightMutex       sync.RWMutex
	pendingQoS2Messages map[uint16]*PendingQoS2Message // messageID -> message (QoS2 messages waiting for PUBREL)
	pendingQoS2Mutex    sync.RWMutex

	// Plugin system
	pluginRegistry      any // Uses any to avoid circular import
	pluginRegistryMutex sync.RWMutex

	// Auth service
	authService      any // Uses any to avoid circular import
	authServiceMutex sync.RWMutex

	// Storage service
	storageService      any // Uses any to avoid circular import
	storageServiceMutex sync.RWMutex

	// Cluster service
	clusterService      any // Uses any to avoid circular import
	clusterServiceMutex sync.RWMutex

	// Rate limiter
	rateLimiter *RateLimiter

	// Message retention configuration
	messageRetention time.Duration // How long to keep messages in storage (0 = forever)

	// Server state
	serverState map[string]any
}

// RetainedMessage represents a message that should be stored and sent to new subscribers
type RetainedMessage struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Modified time.Time
}

// InflightMessage represents a message that has been sent to a client but not yet acknowledged
type InflightMessage struct {
	MessageID    uint16
	ClientID     string
	Topic        string
	Payload      []byte
	QoS          byte
	SentTime     time.Time
	RetryCount   int
	Acknowledged bool
}

// PendingQoS2Message represents a QoS2 message that we've received a PUBLISH for and sent a PUBREC
// but haven't received the PUBREL yet
type PendingQoS2Message struct {
	MessageID    uint16
	ClientID     string
	Topic        string
	Payload      []byte
	ReceivedTime time.Time
}

// NewServer creates a new MQTT broker server instance
func NewServer(host string, port int) *Server {
	return &Server{
		Host:                host,
		Port:                port,
		TLSEnabled:          false,
		TLSPort:             8883,
		WSEnabled:           false,   // WebSocket disabled by default
		WSHost:              host,    // Default to same host as TCP
		WSPort:              9001,    // Default WebSocket port for MQTT
		WSPath:              "/mqtt", // Default WebSocket path
		WSSTLSEnabled:       false,
		WSSTLSPort:          9443,
		clients:             make(map[string]*Client),
		subscriptions:       make(map[string][]*Subscription),
		retainedMessages:    make(map[string]RetainedMessage),
		inflightMessages:    make(map[string]map[uint16]*InflightMessage),
		pendingQoS2Messages: make(map[uint16]*PendingQoS2Message),
		messageRetention:    24 * time.Hour, // Default: store messages for 24 hours
		wsUpgrader: &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins by default
			},
		},
		serverState: make(map[string]any),
		// Initialize rate limiter with default settings
		rateLimiter: NewRateLimiter(
			true,  // Enabled by default
			5,     // 5 connections per second
			100,   // 100 publish messages per second
			20,    // 20 subscriptions per second
			16384, // 16KB message size limit
			2,     // Burst multiplier of 2
		),
	}
}

// EnableTLS enables TLS support for secure MQTT connections (MQTTS)
func (s *Server) EnableTLS(port int, certFile, keyFile string) {
	s.TLSEnabled = true

	if port > 0 {
		s.TLSPort = port
	}

	s.TLSCertFile = certFile
	s.TLSKeyFile = keyFile

	log.Printf("TLS transport enabled on %s:%d", s.Host, s.TLSPort)
}

// EnableClientCertVerification enables client certificate verification (mutual TLS)
func (s *Server) EnableClientCertVerification(caCertFile string) {
	s.TLSRequireClientCert = true
	s.TLSCACertFile = caCertFile

	log.Printf("Client certificate verification enabled with CA file: %s", caCertFile)
}

// EnableWebSocket enables WebSocket support for the MQTT server
func (s *Server) EnableWebSocket(host string, port int, path string) {
	s.WSEnabled = true

	if host != "" {
		s.WSHost = host
	}

	if port > 0 {
		s.WSPort = port
	}

	if path != "" {
		s.WSPath = path
	}

	log.Printf("WebSocket transport enabled on %s:%d%s", s.WSHost, s.WSPort, s.WSPath)
}

// EnableSecureWebSocket enables secure WebSocket (WSS) support for the MQTT server
func (s *Server) EnableSecureWebSocket(port int, certFile, keyFile string) {
	s.WSSTLSEnabled = true

	if port > 0 {
		s.WSSTLSPort = port
	}

	s.WSSTLSCertFile = certFile
	s.WSSTLSKeyFile = keyFile

	log.Printf("Secure WebSocket transport enabled on %s:%d%s", s.WSHost, s.WSSTLSPort, s.WSPath)
}

// Start starts the MQTT server (both TCP and WebSocket if enabled)
func (s *Server) Start() error {
	// Start TCP server
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start MQTT TCP server: %w", err)
	}
	s.listener = listener

	// Start TLS server if enabled
	if s.TLSEnabled {
		go s.startTLSServer()
	}

	// Start message retry mechanism
	s.startMessageRetry()

	log.Printf("MQTT server (TCP) started on %s\n", addr)

	// Start WebSocket server if enabled
	if s.WSEnabled {
		go s.startWebSocketServer()
	}

	// Start Secure WebSocket server if enabled
	if s.WSSTLSEnabled {
		go s.startSecureWebSocketServer()
	}

	// Accept TCP connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting TCP connection: %v\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// startTLSServer starts the TLS server for secure MQTT connections
func (s *Server) startTLSServer() {
	// Load TLS certificate
	cert, err := tls.LoadX509KeyPair(s.TLSCertFile, s.TLSKeyFile)
	if err != nil {
		log.Printf("Error loading TLS certificate: %v\n", err)
		return
	}

	// Configure TLS
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Set up client certificate verification if enabled
	if s.TLSRequireClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

		// Load CA certificate for client cert verification
		if s.TLSCACertFile != "" {
			caCert, err := os.ReadFile(s.TLSCACertFile)
			if err != nil {
				log.Printf("Error loading CA certificate: %v\n", err)
				return
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				log.Printf("Failed to append CA certificate to pool\n")
				return
			}

			tlsConfig.ClientCAs = caCertPool
		}
	}

	// Start TLS listener
	tlsAddr := fmt.Sprintf("%s:%d", s.Host, s.TLSPort)
	tlsListener, err := tls.Listen("tcp", tlsAddr, tlsConfig)
	if err != nil {
		log.Printf("Error starting TLS server: %v\n", err)
		return
	}
	s.tlsListener = tlsListener

	log.Printf("MQTT server (TLS) started on %s\n", tlsAddr)

	// Accept TLS connections
	for {
		conn, err := tlsListener.Accept()
		if err != nil {
			log.Printf("Error accepting TLS connection: %v\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// startWebSocketServer starts the WebSocket server
func (s *Server) startWebSocketServer() {
	// Define WebSocket handler
	handler := http.NewServeMux()
	handler.HandleFunc(s.WSPath, s.handleWebSocket)

	// Create HTTP server
	wsAddr := fmt.Sprintf("%s:%d", s.WSHost, s.WSPort)
	s.wsServer = &http.Server{
		Addr:    wsAddr,
		Handler: handler,
	}

	log.Printf("MQTT server (WebSocket) started on %s%s\n", wsAddr, s.WSPath)

	// Start HTTP server for WebSocket
	if err := s.wsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("Error starting WebSocket server: %v\n", err)
	}
}

// startSecureWebSocketServer starts the WebSocket server with TLS
func (s *Server) startSecureWebSocketServer() {
	// Define WebSocket handler
	handler := http.NewServeMux()
	handler.HandleFunc(s.WSPath, s.handleWebSocket)

	// Create HTTPS server
	wssAddr := fmt.Sprintf("%s:%d", s.WSHost, s.WSSTLSPort)
	s.wssServer = &http.Server{
		Addr:    wssAddr,
		Handler: handler,
	}

	log.Printf("MQTT server (Secure WebSocket) started on %s%s\n", wssAddr, s.WSPath)

	// Start HTTPS server for secure WebSocket
	if err := s.wssServer.ListenAndServeTLS(s.WSSTLSCertFile, s.WSSTLSKeyFile); err != nil && err != http.ErrServerClosed {
		log.Printf("Error starting Secure WebSocket server: %v\n", err)
	}
}

// handleWebSocket handles WebSocket connection upgrade and wraps it as a net.Conn
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade connection to WebSocket
	wsConn, err := s.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v\n", err)
		return
	}

	// Create adapter to convert WebSocket to net.Conn
	conn := &WebSocketConn{
		conn: wsConn,
	}

	// Handle connection like a normal TCP connection
	s.handleConnection(conn)
}

// Stop stops the MQTT server (TCP, TLS, and WebSocket servers)
func (s *Server) Stop() error {
	var err error

	// Stop TCP listener
	if s.listener != nil {
		err = s.listener.Close()
	}

	// Stop TLS listener
	if s.TLSEnabled && s.tlsListener != nil {
		if tlsErr := s.tlsListener.Close(); tlsErr != nil {
			log.Printf("Error closing TLS listener: %v", tlsErr)
			if err == nil {
				err = tlsErr
			}
		}
	}

	// Stop WebSocket server
	if s.WSEnabled && s.wsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if shutdownErr := s.wsServer.Shutdown(ctx); shutdownErr != nil {
			log.Printf("Error shutting down WebSocket server: %v", shutdownErr)
			if err == nil {
				err = shutdownErr
			}
		}
	}

	// Stop Secure WebSocket server
	if s.WSSTLSEnabled && s.wssServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if shutdownErr := s.wssServer.Shutdown(ctx); shutdownErr != nil {
			log.Printf("Error shutting down Secure WebSocket server: %v", shutdownErr)
			if err == nil {
				err = shutdownErr
			}
		}
	}

	return err
}

// WebSocketConn adapts a websocket.Conn to a net.Conn interface
type WebSocketConn struct {
	conn       *websocket.Conn
	readBuf    bytes.Buffer
	nextReader io.Reader
	readMu     sync.Mutex
	writeMu    sync.Mutex
	closed     bool
}

// Read reads data from the WebSocket connection
func (w *WebSocketConn) Read(b []byte) (int, error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	if w.closed {
		return 0, io.EOF
	}

	// If we have data in the buffer, read from it
	if w.readBuf.Len() > 0 {
		return w.readBuf.Read(b)
	}

	// If we have a reader from a previous message, use it
	if w.nextReader != nil {
		n, err := w.nextReader.Read(b)
		if err == io.EOF {
			w.nextReader = nil
			if n > 0 {
				return n, nil
			}
		} else if err != nil {
			return n, err
		} else {
			return n, nil
		}
	}

	// Get the next message
	messageType, reader, err := w.conn.NextReader()
	if err != nil {
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return 0, io.EOF
		}
		return 0, err
	}

	// Only accept binary messages for MQTT
	if messageType != websocket.BinaryMessage {
		log.Printf("Received non-binary WebSocket message, ignoring")
		return 0, nil
	}

	// Store reader for subsequent reads
	w.nextReader = reader

	// Recursively call Read to use the reader we just got
	return w.Read(b)
}

// Write writes data to the WebSocket connection
func (w *WebSocketConn) Write(b []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	// Send as binary message
	err := w.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}

	return len(b), nil
}

// Close closes the WebSocket connection
func (w *WebSocketConn) Close() error {
	w.closed = true
	return w.conn.Close()
}

// LocalAddr returns the local network address
func (w *WebSocketConn) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (w *WebSocketConn) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (w *WebSocketConn) SetDeadline(t time.Time) error {
	if err := w.SetReadDeadline(t); err != nil {
		return err
	}
	return w.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline
func (w *WebSocketConn) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (w *WebSocketConn) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

// handleConnection processes a new client connection
func (s *Server) handleConnection(conn net.Conn) {
	log.Printf("New connection from %s\n", conn.RemoteAddr().String())
	defer func() {
		conn.Close()

		// Find the client for this connection
		var clientToRemove *Client
		var clientID string

		s.clientsMutex.RLock()
		for id, client := range s.clients {
			if client.Conn == conn {
				clientToRemove = client
				clientID = id
				break
			}
		}
		s.clientsMutex.RUnlock()

		// If the client exists, process its Will message and clean up
		if clientToRemove != nil {
			// Check if we should trigger the Will message
			// If the client sent a DISCONNECT packet, we won't trigger the Will
			// Otherwise, it was an unexpected disconnection and we should trigger it
			if clientToRemove.IsConnected {
				log.Printf("Client %s disconnected unexpectedly, processing Will message", clientID)

				// Trigger disconnect event for plugins
				s.triggerPluginEvent("client.disconnect", map[string]any{
					"ClientID":  clientID,
					"Username":  clientToRemove.Username,
					"Timestamp": time.Now().Unix(),
					"Reason":    "unexpected",
				})

				// Publish the Will message if present
				if clientToRemove.WillTopic != "" && clientToRemove.ProcessWill() {
					// Check if the will message should be retained
					if clientToRemove.WillRetain {
						s.storeRetainedMessage(
							clientToRemove.WillTopic,
							clientToRemove.WillMessage,
							clientToRemove.WillQoS,
						)
					}

					// Distribute the will message to subscribers
					s.distributeMessage(
						clientToRemove.WillTopic,
						clientToRemove.WillMessage,
						clientToRemove.WillQoS,
					)
				}
			}

			// Mark client as disconnected
			clientToRemove.IsConnected = false

			// Remove the client unless it requested a persistent session
			// For now, we'll just remove all clients (clean session = true always)
			s.clientsMutex.Lock()
			delete(s.clients, clientID)
			s.clientsMutex.Unlock()

			log.Printf("Client %s removed", clientID)
		}
	}()

	// Use a buffered reader to read from the connection
	reader := bufio.NewReader(conn)

	for {
		// Read and parse packet
		packet, err := ReadPacket(reader)
		if err != nil {
			log.Printf("Connection closed from %s: %v\n", conn.RemoteAddr().String(), err)
			break
		}

		// Process packet based on its type
		switch packet.PacketType {
		case CONNECT:
			s.handleConnect(conn, packet)
		case PUBLISH:
			s.handlePublish(conn, packet)
		case PUBACK:
			s.handlePubAck(conn, packet)
		case PUBREC:
			s.handlePubRec(conn, packet)
		case PUBREL:
			s.handlePubRel(conn, packet)
		case PUBCOMP:
			s.handlePubComp(conn, packet)
		case SUBSCRIBE:
			s.handleSubscribe(conn, packet)
		case UNSUBSCRIBE:
			s.handleUnsubscribe(conn, packet)
		case PINGREQ:
			s.handlePing(conn)
		case DISCONNECT:
			log.Printf("Client disconnected gracefully")

			// Find the client
			var clientID string
			var client *Client

			s.clientsMutex.RLock()
			for id, c := range s.clients {
				if c.Conn == conn {
					clientID = id
					client = c
					// Graceful disconnect - don't trigger the Will message
					client.IsConnected = false
					break
				}
			}
			s.clientsMutex.RUnlock()

			// Trigger plugin event for client disconnection
			if client != nil {
				s.triggerPluginEvent("client.disconnect", map[string]any{
					"ClientID":  clientID,
					"Username":  client.Username,
					"Timestamp": time.Now().Unix(),
					"Reason":    "graceful",
				})
			}

			return
		default:
			log.Printf("Unsupported packet type: %d\n", packet.PacketType)
		}
	}
}

// handleConnect processes a CONNECT packet
func (s *Server) handleConnect(conn net.Conn, packet *Packet) {
	// Extract client ID from packet
	clientID := string(packet.ClientID)
	if clientID == "" {
		// Auto-generate client ID if not provided
		clientID = fmt.Sprintf("client-%s", conn.RemoteAddr().String())
	}

	// Check rate limiting for connection
	if !s.rateLimiter.Allow(clientID, rate.ConnectLimit) {
		log.Printf("Connection rate limit exceeded for client %s", clientID)
		// Send connection refused response with appropriate reason code
		// The exact implementation depends on the Packet structure
		// For now, just close the connection
		conn.Close()
		return
	}

	log.Printf("Client %s connected with protocol %s v%d\n",
		clientID, packet.ProtocolName, packet.ProtocolVersion)

	// TODO: Implement authentication here using Username and Password from packet

	// Create CONNACK packet
	connack := NewConnAckPacket(false, ConnAccepted) // No session present, connection accepted
	connackBytes, err := connack.Encode()
	if err != nil {
		log.Printf("Error encoding CONNACK: %v\n", err)
		return
	}

	_, err = conn.Write(connackBytes)
	if err != nil {
		log.Printf("Error sending CONNACK: %v\n", err)
		return
	}

	// Create and store client
	client := NewClient(clientID, conn)

	// Save Will message if present
	if packet.WillTopic != "" {
		client.WillTopic = packet.WillTopic
		client.WillMessage = packet.WillMessage
		client.WillQoS = packet.WillQoS
		client.WillRetain = packet.WillRetain
	}

	s.clientsMutex.Lock()
	s.clients[clientID] = client
	s.clientsMutex.Unlock()

	log.Printf("Client %s connected and registered\n", clientID)

	// Trigger plugin event for client connection
	s.triggerPluginEvent("client.connect", map[string]any{
		"ClientID":   clientID,
		"Username":   packet.Username,
		"Timestamp":  time.Now().Unix(),
		"RemoteAddr": conn.RemoteAddr().String(),
	})
}

// handlePublish processes a PUBLISH packet
func (s *Server) handlePublish(conn net.Conn, packet *Packet) {
	// Get client ID
	clientID := ""
	s.clientsMutex.RLock()
	for id, client := range s.clients {
		if client.Conn == conn {
			clientID = id
			break
		}
	}
	s.clientsMutex.RUnlock()

	if clientID == "" {
		log.Println("Unable to find client for connection")
		return
	}

	// Check publish rate limiting
	if !s.rateLimiter.Allow(clientID, rate.PublishLimit) {
		log.Printf("Publish rate limit exceeded for client %s", clientID)

		// For MQTT 5.0, we could send back an error
		// For now, we'll just ignore the publish (the client will eventually resend for QoS > 0)
		return
	}

	// Check message size rate limiting if payload is large
	if len(packet.Payload) > 0 && !s.rateLimiter.Allow(clientID, rate.MessageSizeLimit) {
		log.Printf("Message size limit exceeded for client %s", clientID)
		return
	}

	// Extract topic and payload
	topic := packet.TopicName
	payload := packet.Payload
	qos := packet.Qos
	retained := packet.Retain
	messageID := packet.MessageID

	// Check topic permissions
	err := s.checkTopicPermission(clientID, topic, true)
	if err != nil {
		log.Printf("Client %s not authorized to publish to %s: %v", clientID, topic, err)
		return
	}

	// For QoS 1, send PUBACK
	if qos == 1 {
		puback := NewPubAckPacket(messageID)
		pubackBytes, err := puback.Encode()
		if err != nil {
			log.Printf("Error encoding PUBACK: %v", err)
		} else {
			_, err = conn.Write(pubackBytes)
			if err != nil {
				log.Printf("Error sending PUBACK: %v", err)
			}
		}
	}

	// For QoS 2, send PUBREC and store message
	if qos == 2 {
		pubrec := NewPubRecPacket(messageID)
		pubrecBytes, err := pubrec.Encode()
		if err != nil {
			log.Printf("Error encoding PUBREC: %v", err)
		} else {
			_, err = conn.Write(pubrecBytes)
			if err != nil {
				log.Printf("Error sending PUBREC: %v", err)
			}
		}

		// Store QoS 2 message waiting for PUBREL
		s.pendingQoS2Mutex.Lock()
		s.pendingQoS2Messages[messageID] = &PendingQoS2Message{
			MessageID:    messageID,
			ClientID:     clientID,
			Topic:        topic,
			Payload:      payload,
			ReceivedTime: time.Now(),
		}
		s.pendingQoS2Mutex.Unlock()

		// Don't distribute QoS 2 message yet - wait for PUBREL
		return
	}

	// Store retained message if requested
	if retained {
		s.storeRetainedMessage(topic, payload, qos)
	}

	// Persist message if storage is enabled
	s.persistMessage(clientID, topic, payload, qos, retained)

	// Distribute message to subscribers
	s.distributeMessage(topic, payload, qos)

	// Trigger plugin event for message published
	s.triggerPluginEvent("message.published", map[string]any{
		"ClientID":  clientID,
		"Topic":     topic,
		"QoS":       qos,
		"Retained":  retained,
		"Timestamp": time.Now().Unix(),
		"Size":      len(payload),
	})
}

// persistMessage stores a message in the database if storage is available
func (s *Server) persistMessage(clientID, topic string, payload []byte, qos byte, retained bool) {
	s.storageServiceMutex.RLock()
	storage := s.storageService
	s.storageServiceMutex.RUnlock()

	if storage == nil {
		return
	}

	// Use reflection to call StoreMessage on the storage service
	storageValue := reflect.ValueOf(storage)
	storeMethod := storageValue.MethodByName("StoreMessage")

	if !storeMethod.IsValid() {
		return
	}

	// Create a simplified message structure that can be converted by reflection
	type StorageMessage struct {
		Topic     string
		Payload   []byte
		QoS       byte
		Retained  bool
		ClientID  string
		Timestamp time.Time
	}

	// Populate the message with the data we have
	msg := &StorageMessage{
		Topic:     topic,
		Payload:   payload,
		QoS:       qos,
		Retained:  retained,
		ClientID:  clientID,
		Timestamp: time.Now(),
	}

	// Call the StoreMessage method with message and retention
	res := storeMethod.Call([]reflect.Value{
		reflect.ValueOf(msg),
		reflect.ValueOf(s.messageRetention),
	})

	// Check for errors
	if len(res) > 0 && !res[0].IsNil() {
		log.Printf("Error storing message in database: %v", res[0].Interface())
	}
}

// handlePubAck processes a PUBACK packet (QoS 1 acknowledgment)
func (s *Server) handlePubAck(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBACK for message ID %d", packet.MessageID)

	// Find client from connection
	var clientID string
	s.clientsMutex.RLock()
	for id, client := range s.clients {
		if client.Conn == conn {
			clientID = id
			break
		}
	}
	s.clientsMutex.RUnlock()

	if clientID != "" {
		// Remove the message from our inflight store
		s.removeInflightMessage(clientID, packet.MessageID)
	}
}

// handlePubRec processes a PUBREC packet (first acknowledgment in QoS 2 flow)
func (s *Server) handlePubRec(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBREC for message ID %d", packet.MessageID)

	// Find client from connection
	var clientID string
	s.clientsMutex.RLock()
	for id, client := range s.clients {
		if client.Conn == conn {
			clientID = id
			break
		}
	}
	s.clientsMutex.RUnlock()

	if clientID != "" {
		// Mark the message as acknowledged in our inflight store
		// We don't remove it yet because we need to wait for PUBCOMP
		s.acknowledgeInflightMessage(clientID, packet.MessageID)
	}

	// Send PUBREL in response
	pubrel := NewPubRelPacket(packet.MessageID)
	pubrelBytes, err := pubrel.Encode()
	if err != nil {
		log.Printf("Error encoding PUBREL: %v\n", err)
		return
	}

	_, err = conn.Write(pubrelBytes)
	if err != nil {
		log.Printf("Error sending PUBREL: %v\n", err)
	}
}

// handlePubRel processes a PUBREL packet (second packet in QoS 2 flow)
func (s *Server) handlePubRel(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBREL for message ID %d", packet.MessageID)

	// Get the pending QoS2 message
	s.pendingQoS2Mutex.Lock()
	pendingMsg, exists := s.pendingQoS2Messages[packet.MessageID]
	if exists {
		// Remove it from pending map
		delete(s.pendingQoS2Messages, packet.MessageID)
	}
	s.pendingQoS2Mutex.Unlock()

	// If we found a pending message, now we can distribute it
	if exists {
		log.Printf("Distributing QoS2 message (ID: %d) after receiving PUBREL", packet.MessageID)
		// Now that we've received PUBREL, we can deliver the message to subscribers
		s.distributeMessage(pendingMsg.Topic, pendingMsg.Payload, QoS2)
	}

	// Send PUBCOMP to acknowledge
	pubcomp := NewPubCompPacket(packet.MessageID)
	pubcompBytes, err := pubcomp.Encode()
	if err != nil {
		log.Printf("Error encoding PUBCOMP: %v\n", err)
		return
	}

	_, err = conn.Write(pubcompBytes)
	if err != nil {
		log.Printf("Error sending PUBCOMP: %v\n", err)
	}
}

// handlePubComp processes a PUBCOMP packet (final acknowledgment in QoS 2 flow)
func (s *Server) handlePubComp(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBCOMP for message ID %d", packet.MessageID)

	// Find client from connection
	var clientID string
	s.clientsMutex.RLock()
	for id, client := range s.clients {
		if client.Conn == conn {
			clientID = id
			break
		}
	}
	s.clientsMutex.RUnlock()

	if clientID != "" {
		// Now we can completely remove the message from our inflight store
		s.removeInflightMessage(clientID, packet.MessageID)
	}
}

// handleSubscribe processes a SUBSCRIBE packet
func (s *Server) handleSubscribe(conn net.Conn, packet *Packet) {
	// Find client ID from connection
	var clientID string
	var client *Client

	s.clientsMutex.RLock()
	for id, c := range s.clients {
		if c.Conn == conn {
			clientID = id
			client = c
			break
		}
	}
	s.clientsMutex.RUnlock()

	if client == nil {
		log.Println("Unable to find client for subscribe")
		return
	}

	// Check subscribe rate limiting
	if !s.rateLimiter.Allow(clientID, rate.SubscribeLimit) {
		log.Printf("Subscribe rate limit exceeded for client %s", clientID)

		// For MQTT 5.0, we could return error codes in SUBACK
		// For MQTT 3.1.1, we'll return failure codes for each topic

		// Create SUBACK with all failure codes
		qosLevels := make([]byte, len(packet.Topics))
		for i := range qosLevels {
			qosLevels[i] = 0x80 // 0x80 = Failure
		}

		suback := NewSubAckPacket(packet.MessageID, qosLevels)
		subackBytes, err := suback.Encode()
		if err != nil {
			log.Printf("Error encoding SUBACK: %v", err)
			return
		}

		_, err = conn.Write(subackBytes)
		if err != nil {
			log.Printf("Error sending SUBACK: %v", err)
		}
		return
	}

	log.Printf("Client %s subscribing to topics: %v\n", clientID, packet.Topics)

	// Process subscription requests
	var grantedQoS []byte
	var newSubscriptions []string

	for i, topic := range packet.Topics {
		var qos byte = 0
		if i < len(packet.QoSs) {
			qos = packet.QoSs[i]
		}

		// Check subscribe permission (read access)
		if err := s.checkTopicPermission(clientID, topic, false); err != nil {
			log.Printf("Permission denied for client %s to subscribe to topic %s: %v",
				clientID, topic, err)
			// When permission is denied, we grant QoS 0x80 (subscription failure)
			grantedQoS = append(grantedQoS, 0x80)
			continue
		}

		// Store subscription
		subscription := client.Subscribe(topic, qos)

		// Add to server's subscription map
		s.subMutex.Lock()
		s.subscriptions[topic] = append(s.subscriptions[topic], subscription)
		s.subMutex.Unlock()

		// Track new subscriptions for retained message delivery
		newSubscriptions = append(newSubscriptions, topic)

		// Grant requested QoS
		grantedQoS = append(grantedQoS, qos)
	}

	// Send SUBACK
	suback := NewSubAckPacket(packet.MessageID, grantedQoS)
	subackBytes, err := suback.Encode()
	if err != nil {
		log.Printf("Error encoding SUBACK: %v\n", err)
		return
	}

	_, err = conn.Write(subackBytes)
	if err != nil {
		log.Printf("Error sending SUBACK: %v\n", err)
	}

	// Send retained messages that match the new subscriptions
	s.sendRetainedMessages(client, newSubscriptions)

	// Broadcast subscription to cluster if enabled
	if s.clusterService != nil {
		// Use reflection to call BroadcastSubscription without direct import
		if broadcaster, ok := s.clusterService.(interface {
			BroadcastSubscription(clientID, topic string, qos byte)
		}); ok {
			for i, topic := range packet.Topics {
				var qos byte = 0
				if i < len(packet.QoSs) {
					qos = packet.QoSs[i]
				}
				// Only broadcast successful subscriptions (not failed ones with QoS 0x80)
				if i < len(grantedQoS) && grantedQoS[i] != 0x80 {
					broadcaster.BroadcastSubscription(client.ID, topic, qos)
				}
			}
		}
	}
}

// sendRetainedMessages sends retained messages to a client for its subscriptions
func (s *Server) sendRetainedMessages(client *Client, subscriptions []string) {
	// Skip if no client or no subscriptions
	if client == nil || len(subscriptions) == 0 {
		return
	}

	s.retainedMessagesMutex.RLock()
	defer s.retainedMessagesMutex.RUnlock()

	// Fast path if no retained messages
	if len(s.retainedMessages) == 0 {
		return
	}

	// Check each retained message against each subscription
	for _, subTopic := range subscriptions {
		for retainedTopic, retainedMsg := range s.retainedMessages {
			if topicMatches(subTopic, retainedTopic) {
				// Find the subscription to get the client's requested QoS
				var subscriptionQoS byte = 0
				client.subMutex.RLock()
				if sub, ok := client.Subscriptions[subTopic]; ok {
					subscriptionQoS = sub.QoS
				}
				client.subMutex.RUnlock()

				// Choose the lower QoS between retained message and subscription
				effectiveQoS := retainedMsg.QoS
				if subscriptionQoS < effectiveQoS {
					effectiveQoS = subscriptionQoS
				}

				// Create a PUBLISH packet with the retain flag set
				publish := NewPublishPacket(retainedTopic, retainedMsg.Payload, effectiveQoS, 0, false, true)
				publishBytes, err := publish.Encode()
				if err != nil {
					log.Printf("Error encoding retained PUBLISH for client %s: %v\n", client.ID, err)
					continue
				}

				// Send to client
				_, err = client.Conn.Write(publishBytes)
				if err != nil {
					log.Printf("Error sending retained PUBLISH to client %s: %v\n", client.ID, err)
				} else {
					log.Printf("Sent retained message on topic %s to client %s", retainedTopic, client.ID)
				}
			}
		}
	}
}

// handleUnsubscribe processes an UNSUBSCRIBE packet
func (s *Server) handleUnsubscribe(conn net.Conn, packet *Packet) {
	// Find client
	var clientID string
	var client *Client

	s.clientsMutex.RLock()
	for id, c := range s.clients {
		if c.Conn == conn {
			clientID = id
			client = c
			break
		}
	}
	s.clientsMutex.RUnlock()

	if client == nil {
		log.Printf("Unknown client tried to unsubscribe\n")
		return
	}

	log.Printf("Client %s unsubscribing from topics: %v\n", clientID, packet.Topics)

	// Process unsubscribe requests
	for _, topic := range packet.Topics {
		// Remove from client's subscriptions
		client.Unsubscribe(topic)

		// Remove from server's subscription map
		s.subMutex.Lock()
		var remainingSubs []*Subscription
		for _, sub := range s.subscriptions[topic] {
			if sub.ClientID != clientID {
				remainingSubs = append(remainingSubs, sub)
			}
		}

		if len(remainingSubs) > 0 {
			s.subscriptions[topic] = remainingSubs
		} else {
			delete(s.subscriptions, topic)
		}
		s.subMutex.Unlock()
	}

	// Send UNSUBACK
	unsuback := NewUnsubAckPacket(packet.MessageID)
	unsubackBytes, err := unsuback.Encode()
	if err != nil {
		log.Printf("Error encoding UNSUBACK: %v\n", err)
		return
	}

	_, err = conn.Write(unsubackBytes)
	if err != nil {
		log.Printf("Error sending UNSUBACK: %v\n", err)
	}

	// Broadcast unsubscription to cluster if enabled
	if s.clusterService != nil {
		// Use reflection to call BroadcastUnsubscription without direct import
		if broadcaster, ok := s.clusterService.(interface {
			BroadcastUnsubscription(clientID, topic string)
		}); ok {
			for _, topic := range packet.Topics {
				broadcaster.BroadcastUnsubscription(client.ID, topic)
			}
		}
	}
}

// handlePing responds to a PINGREQ with a PINGRESP
func (s *Server) handlePing(conn net.Conn) {
	pingResp := NewPingRespPacket()
	pingRespBytes, err := pingResp.Encode()
	if err != nil {
		log.Printf("Error encoding PINGRESP: %v\n", err)
		return
	}

	_, err = conn.Write(pingRespBytes)
	if err != nil {
		log.Printf("Error sending PINGRESP: %v\n", err)
	}
}

// distributeMessage sends a published message to all relevant subscribers
func (s *Server) distributeMessage(topic string, payload []byte, qos byte) {
	s.subMutex.RLock()
	defer s.subMutex.RUnlock()

	// Group subscriptions by shared subscription groups
	// Map of shareGroup -> subTopic -> []*Subscription
	sharedSubs := make(map[string]map[string][]*Subscription)

	// Find matching subscriptions using topic matching
	for subTopic, subscriptions := range s.subscriptions {
		if topicMatches(subTopic, topic) {
			// Group shared subscriptions for later processing
			regularSubs := make([]*Subscription, 0)

			for _, subscription := range subscriptions {
				if subscription.IsShared {
					// Initialize maps if needed
					if _, ok := sharedSubs[subscription.ShareGroup]; !ok {
						sharedSubs[subscription.ShareGroup] = make(map[string][]*Subscription)
					}

					// Add to shared subscriptions map
					sharedSubs[subscription.ShareGroup][subTopic] = append(
						sharedSubs[subscription.ShareGroup][subTopic],
						subscription,
					)
				} else {
					// Regular subscription
					regularSubs = append(regularSubs, subscription)
				}
			}

			// Process regular subscriptions immediately
			for _, subscription := range regularSubs {
				s.deliverMessageToSubscriber(subscription, topic, payload, qos)
			}
		}
	}

	// Process shared subscriptions - one client per share group gets the message
	for shareGroup, topicMap := range sharedSubs {
		for subTopic, subscriptions := range topicMap {
			// Skip empty subscription lists
			if len(subscriptions) == 0 {
				continue
			}

			// Randomly select one subscriber from the share group
			// This is a simple round-robin approach, but could be extended with more sophisticated strategies
			selectedIndex := rand.Intn(len(subscriptions))
			selectedSub := subscriptions[selectedIndex]

			log.Printf("Selected subscriber %s from share group %s for topic %s",
				selectedSub.ClientID, shareGroup, subTopic)

			// Deliver to the selected subscriber
			s.deliverMessageToSubscriber(selectedSub, topic, payload, qos)
		}
	}
}

// deliverMessageToSubscriber delivers a message to a specific subscriber
func (s *Server) deliverMessageToSubscriber(subscription *Subscription, topic string, payload []byte, qos byte) {
	// Find the client
	s.clientsMutex.RLock()
	client, ok := s.clients[subscription.ClientID]
	s.clientsMutex.RUnlock()

	if !ok || !client.IsConnected {
		return
	}

	// Skip if NoLocal is true and this client published the message
	// This would require passing the source client ID to this function for complete implementation

	// Choose the lower QoS between subscription QoS and publish QoS
	effectiveQoS := qos
	if subscription.QoS < qos {
		effectiveQoS = subscription.QoS
	}

	// Generate message ID for QoS > 0
	var messageID uint16 = 0
	if effectiveQoS > 0 {
		messageID = s.generateMessageID(client.ID)
	}

	// Create PUBLISH packet
	publish := NewPublishPacket(topic, payload, effectiveQoS, messageID, false, false)
	publishBytes, err := publish.Encode()
	if err != nil {
		log.Printf("Error encoding PUBLISH for client %s: %v\n",
			subscription.ClientID, err)
		return
	}

	// For QoS1 and QoS2, store the message as in-flight
	if effectiveQoS > 0 {
		s.storeInflightMessage(client.ID, messageID, topic, payload, effectiveQoS)
	}

	// Send to client
	_, err = client.Conn.Write(publishBytes)
	if err != nil {
		log.Printf("Error sending PUBLISH to client %s: %v\n",
			subscription.ClientID, err)
	} else {
		log.Printf("Sent PUBLISH to client %s: topic=%s, qos=%d, messageID=%d",
			subscription.ClientID, topic, effectiveQoS, messageID)

		// Trigger plugin event for message receive
		s.triggerPluginEvent("message.receive", map[string]any{
			"ClientID":  subscription.ClientID,
			"Username":  client.Username,
			"Topic":     topic,
			"Payload":   payload,
			"QoS":       effectiveQoS,
			"Timestamp": time.Now().Unix(),
		})
	}
}

// generateMessageID generates a unique message ID for a client
func (s *Server) generateMessageID(clientID string) uint16 {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	// Make sure the client map exists
	if _, ok := s.inflightMessages[clientID]; !ok {
		s.inflightMessages[clientID] = make(map[uint16]*InflightMessage)
	}

	// Track the last used message ID for this client
	lastIDKey := fmt.Sprintf("%s_lastMsgID", clientID)

	// Retrieve the last ID used for this client (defaulting to 0)
	var lastID uint16
	if val, exists := s.serverState[lastIDKey]; exists {
		if id, ok := val.(uint16); ok {
			lastID = id
		}
	}

	// Start from the last used ID + 1
	messageID := lastID + 1
	if messageID == 0 { // Avoid messageID 0 (wrap-around)
		messageID = 1
	}

	// Find an available ID if this one is already in use
	originalID := messageID
	for {
		if _, exists := s.inflightMessages[clientID][messageID]; !exists {
			break
		}
		messageID++
		if messageID == 0 { // Avoid messageID 0 (wrap-around)
			messageID = 1
		}
		// Prevent infinite loop if all IDs are somehow in use
		if messageID == originalID {
			log.Printf("Warning: All message IDs are in use for client %s", clientID)
			break
		}
	}

	// Store the new ID as the last used ID for this client
	s.serverState[lastIDKey] = messageID

	return messageID
}

// storeInflightMessage stores a message that's been sent to a client and waiting for ack
func (s *Server) storeInflightMessage(clientID string, messageID uint16, topic string, payload []byte, qos byte) {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	// Make sure the client map exists
	if _, ok := s.inflightMessages[clientID]; !ok {
		s.inflightMessages[clientID] = make(map[uint16]*InflightMessage)
	}

	// Store the message
	s.inflightMessages[clientID][messageID] = &InflightMessage{
		MessageID:    messageID,
		ClientID:     clientID,
		Topic:        topic,
		Payload:      payload,
		QoS:          qos,
		SentTime:     time.Now(),
		RetryCount:   0,
		Acknowledged: false,
	}

	log.Printf("Stored inflight message: client=%s, messageID=%d, qos=%d", clientID, messageID, qos)
}

// acknowledgeInflightMessage marks a message as acknowledged
func (s *Server) acknowledgeInflightMessage(clientID string, messageID uint16) {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	// Check if the message exists
	if clientMessages, ok := s.inflightMessages[clientID]; ok {
		if message, exists := clientMessages[messageID]; exists {
			message.Acknowledged = true
			log.Printf("Acknowledged inflight message: client=%s, messageID=%d", clientID, messageID)
		}
	}
}

// removeInflightMessage removes a message from the inflight store
func (s *Server) removeInflightMessage(clientID string, messageID uint16) {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	// Remove the message if it exists
	if clientMessages, ok := s.inflightMessages[clientID]; ok {
		delete(clientMessages, messageID)
		log.Printf("Removed inflight message: client=%s, messageID=%d", clientID, messageID)
	}
}

// topicMatches checks if a subscription topic matches a published topic
// It handles wildcards (+ and #) according to the MQTT spec
func topicMatches(subTopic, pubTopic string) bool {
	// Quick check for exact match
	if subTopic == pubTopic {
		return true
	}

	// Handle shared subscription format - extract the actual topic pattern
	// Format: $share/{share_name}/{topic}
	if strings.HasPrefix(subTopic, "$share/") {
		parts := strings.SplitN(subTopic, "/", 3)
		if len(parts) >= 3 {
			// Use the actual topic filter for matching
			subTopic = parts[2]

			// Check again for exact match after extracting real topic
			if subTopic == pubTopic {
				return true
			}
		} else {
			// Invalid shared subscription format
			return false
		}
	}

	// Split topics into segments
	subSegments := strings.Split(subTopic, "/")
	pubSegments := strings.Split(pubTopic, "/")

	// Special case: # wildcard alone
	if subTopic == "#" {
		return true
	}

	// If subscription ends with #, it matches all remaining levels
	if subSegments[len(subSegments)-1] == "#" {
		// Check all segments before the # against pubTopic
		for i := 0; i < len(subSegments)-1; i++ {
			// If we've run out of publish segments but still have subscription segments
			if i >= len(pubSegments) {
				return false
			}

			// Skip '+' wildcard segments
			if subSegments[i] == "+" {
				continue
			}

			// If segments don't match, no match
			if subSegments[i] != pubSegments[i] {
				return false
			}
		}

		// If we get here, all segments before # matched
		return true
	}

	// No # wildcard, so number of segments must match
	if len(subSegments) != len(pubSegments) {
		return false
	}

	// Check each segment
	for i, segment := range subSegments {
		// + wildcard matches any single segment
		if segment == "+" {
			continue
		}

		// If segments don't match, no match
		if segment != pubSegments[i] {
			return false
		}
	}

	// All segments matched
	return true
}

// startMessageRetry starts a goroutine to periodically check and retry unacknowledged messages
func (s *Server) startMessageRetry() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			s.retryInflightMessages()
		}
	}()
}

// retryInflightMessages checks for unacknowledged messages that need to be retried
func (s *Server) retryInflightMessages() {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	now := time.Now()
	retryInterval := 10 * time.Second // Time to wait before retry
	maxRetries := 3                   // Maximum number of retries

	for clientID, messages := range s.inflightMessages {
		// Get the client
		s.clientsMutex.RLock()
		client, ok := s.clients[clientID]
		s.clientsMutex.RUnlock()

		if !ok || !client.IsConnected {
			// Client disconnected, clean up its messages
			delete(s.inflightMessages, clientID)
			continue
		}

		for messageID, msg := range messages {
			// Skip acknowledged messages
			if msg.Acknowledged {
				continue
			}

			// Check if it's time to retry
			if now.Sub(msg.SentTime) > retryInterval {
				// Check if we've reached max retries
				if msg.RetryCount >= maxRetries {
					log.Printf("Message ID %d to client %s exceeded max retries, giving up",
						messageID, clientID)
					delete(messages, messageID)
					continue
				}

				// Retry the message
				log.Printf("Retrying message ID %d to client %s (retry #%d)",
					messageID, clientID, msg.RetryCount+1)

				// Create a new PUBLISH packet with the DUP flag set
				publish := NewPublishPacket(msg.Topic, msg.Payload, msg.QoS, messageID, true, false)
				publishBytes, err := publish.Encode()
				if err != nil {
					log.Printf("Error encoding retry PUBLISH: %v", err)
					continue
				}

				// Send to client
				_, err = client.Conn.Write(publishBytes)
				if err != nil {
					log.Printf("Error sending retry PUBLISH: %v", err)
					continue
				}

				// Update retry count and time
				msg.RetryCount++
				msg.SentTime = now
			}
		}
	}
}

// storeRetainedMessage stores a retained message
func (s *Server) storeRetainedMessage(topic string, payload []byte, qos byte) {
	// Special case: empty payload means delete the retained message
	if len(payload) == 0 {
		s.retainedMessagesMutex.Lock()
		delete(s.retainedMessages, topic)
		s.retainedMessagesMutex.Unlock()
		log.Printf("Deleted retained message for topic: %s", topic)
		return
	}

	// Store the retained message
	s.retainedMessagesMutex.Lock()
	s.retainedMessages[topic] = RetainedMessage{
		Topic:    topic,
		Payload:  payload,
		QoS:      qos,
		Modified: time.Now(),
	}
	s.retainedMessagesMutex.Unlock()
	log.Printf("Stored retained message for topic: %s", topic)
}

// StoreRetainedMessage stores a retained message (public version)
func (s *Server) StoreRetainedMessage(topic string, payload []byte, qos byte) {
	s.storeRetainedMessage(topic, payload, qos)
}

// GetRetainedMessages iterates through all retained messages and calls the callback function for each one
// The callback should return true to continue iteration or false to stop
func (s *Server) GetRetainedMessages(callback func(topic string, payload []byte, qos byte) bool) {
	s.retainedMessagesMutex.RLock()
	defer s.retainedMessagesMutex.RUnlock()

	for topic, msg := range s.retainedMessages {
		if !callback(topic, msg.Payload, msg.QoS) {
			break
		}
	}
}

// SetPluginRegistry sets the plugin registry for the server
func (s *Server) SetPluginRegistry(registry any) {
	s.pluginRegistryMutex.Lock()
	defer s.pluginRegistryMutex.Unlock()
	s.pluginRegistry = registry
	log.Printf("Plugin registry set for MQTT server")
}

// triggerPluginEvent triggers a plugin event if a plugin registry is set
func (s *Server) triggerPluginEvent(event string, context map[string]any) {
	s.pluginRegistryMutex.RLock()
	registry := s.pluginRegistry
	s.pluginRegistryMutex.RUnlock()

	if registry == nil {
		return
	}

	// Since we can't directly access the plugin package methods due to circular imports,
	// we'll use reflection to call the TriggerEvent method
	// Try to find the TriggerEvent method
	registryValue := reflect.ValueOf(registry)
	triggerMethod := registryValue.MethodByName("TriggerEvent")

	if triggerMethod.IsValid() {
		// Create a context struct that matches what the plugin system expects
		// We'll define this as a map and let reflect handle it
		ctxMap := make(map[string]any)
		ctxMap["Event"] = event

		// Copy the provided context values
		for k, v := range context {
			ctxMap[k] = v
		}

		// Create a new context value
		ctxValue := reflect.ValueOf(ctxMap)

		// Call the method
		triggerMethod.Call([]reflect.Value{ctxValue})
	}
}

// SetAuthService sets the auth service to be used for permission checking
func (s *Server) SetAuthService(auth any) {
	s.authServiceMutex.Lock()
	defer s.authServiceMutex.Unlock()
	s.authService = auth
}

// checkTopicPermission checks if a client has permission to access a topic
// If requireWrite is true, checks for write permission, otherwise read permission
func (s *Server) checkTopicPermission(clientID, topic string, requireWrite bool) error {
	s.authServiceMutex.RLock()
	defer s.authServiceMutex.RUnlock()

	if s.authService == nil {
		// If auth service is not set, allow all operations
		return nil
	}

	// Use reflection to call the CheckTopicPermission method on the auth service
	authValue := reflect.ValueOf(s.authService)
	method := authValue.MethodByName("CheckTopicPermission")

	if !method.IsValid() {
		log.Printf("CheckTopicPermission method not found on auth service")
		return nil
	}

	results := method.Call([]reflect.Value{
		reflect.ValueOf(clientID),
		reflect.ValueOf(topic),
		reflect.ValueOf(requireWrite),
	})

	if len(results) > 0 && !results[0].IsNil() {
		return results[0].Interface().(error)
	}

	return nil
}

// SetStorageService sets the storage service for the MQTT server
func (s *Server) SetStorageService(storage any) {
	s.storageServiceMutex.Lock()
	defer s.storageServiceMutex.Unlock()
	s.storageService = storage
	log.Printf("Storage service set for MQTT server")
}

// SetMessageRetention sets the duration for which messages are kept in storage
func (s *Server) SetMessageRetention(retention time.Duration) {
	s.messageRetention = retention
	log.Printf("Message retention set to %s", retention)
}

// SetClusterService sets the cluster service for this server
func (s *Server) SetClusterService(clusterService any) {
	s.clusterServiceMutex.Lock()
	defer s.clusterServiceMutex.Unlock()

	s.clusterService = clusterService
	log.Printf("Cluster service set for MQTT server")
}

// SetRateLimiting enables or disables rate limiting for the server
func (s *Server) SetRateLimiting(enabled bool) {
	s.rateLimiter.Enable(enabled)
}

// ConfigureRateLimiter configures the rate limiter with new default rates
func (s *Server) ConfigureRateLimiter(connectLimit, publishLimit, subscribeLimit, messageSizeLimit float64, burstMultiplier float64) {
	// Update the default rates
	s.rateLimiter.SetDefaultRate(rate.ConnectLimit, connectLimit)
	s.rateLimiter.SetDefaultRate(rate.PublishLimit, publishLimit)
	s.rateLimiter.SetDefaultRate(rate.SubscribeLimit, subscribeLimit)
	s.rateLimiter.SetDefaultRate(rate.MessageSizeLimit, messageSizeLimit)
}

// SetClientRateLimit sets a custom rate limit for a specific client and limit type
func (s *Server) SetClientRateLimit(clientID string, limitType string, rateLimit float64) {
	s.rateLimiter.SetClientRate(clientID, limitType, rateLimit)
}

// ResetClientRateLimits resets rate limits for a specific client
func (s *Server) ResetClientRateLimits(clientID string) {
	s.rateLimiter.Reset(clientID)
}

// ConfigureRateLimitingFromConfig sets up rate limiting from a configuration object
func (s *Server) ConfigureRateLimitingFromConfig(config any) {
	// First, try to use reflection to get the rate limiting config
	// This approach avoids a direct import dependency on the config package

	enabled := false
	connectLimit := 5.0
	publishLimit := 100.0
	subscribeLimit := 20.0
	burstMultiplier := 2.0

	// Try to access the config fields via reflection
	if configVal := reflect.ValueOf(config); configVal.Kind() == reflect.Ptr && !configVal.IsNil() {
		// Look for MQTT field
		if mqttField := configVal.Elem().FieldByName("MQTT"); mqttField.IsValid() {
			// Look for RateLimiting field
			if rateLimitingField := mqttField.FieldByName("RateLimiting"); rateLimitingField.IsValid() {
				// Get Enabled field
				if enabledField := rateLimitingField.FieldByName("Enabled"); enabledField.IsValid() && enabledField.Kind() == reflect.Bool {
					enabled = enabledField.Bool()
				}

				// Get ConnectLimit field
				if connectLimitField := rateLimitingField.FieldByName("ConnectLimit"); connectLimitField.IsValid() && connectLimitField.Kind() == reflect.Float64 {
					connectLimit = connectLimitField.Float()
				}

				// Get PublishLimit field
				if publishLimitField := rateLimitingField.FieldByName("PublishLimit"); publishLimitField.IsValid() && publishLimitField.Kind() == reflect.Float64 {
					publishLimit = publishLimitField.Float()
				}

				// Get SubscribeLimit field
				if subscribeLimitField := rateLimitingField.FieldByName("SubscribeLimit"); subscribeLimitField.IsValid() && subscribeLimitField.Kind() == reflect.Float64 {
					subscribeLimit = subscribeLimitField.Float()
				}

				// Get BurstMultiplier field
				if burstMultiplierField := rateLimitingField.FieldByName("BurstMultiplier"); burstMultiplierField.IsValid() && burstMultiplierField.Kind() == reflect.Float64 {
					burstMultiplier = burstMultiplierField.Float()
				}
			}
		}
	}

	// Update rate limiter settings
	s.rateLimiter.Enable(enabled)

	// If rate limiting is enabled, update the rates
	if enabled {
		s.ConfigureRateLimiter(connectLimit, publishLimit, subscribeLimit, 16384, burstMultiplier)
	}

	log.Printf("Rate limiting configured: enabled=%v, connect=%v/s, publish=%v/s, subscribe=%v/s, burst=%vx",
		enabled, connectLimit, publishLimit, subscribeLimit, burstMultiplier)
}
