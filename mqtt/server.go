package mqtt

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Server represents the MQTT broker server
type Server struct {
	// Configuration
	Host string
	Port int

	// TCP listener
	listener net.Listener

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

	// Message retention configuration
	messageRetention time.Duration // How long to keep messages in storage (0 = forever)
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
		clients:             make(map[string]*Client),
		subscriptions:       make(map[string][]*Subscription),
		retainedMessages:    make(map[string]RetainedMessage),
		inflightMessages:    make(map[string]map[uint16]*InflightMessage),
		pendingQoS2Messages: make(map[uint16]*PendingQoS2Message),
		messageRetention:    24 * time.Hour, // Default: store messages for 24 hours
	}
}

// Start starts the MQTT server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start MQTT server: %w", err)
	}
	s.listener = listener

	// Start the message retry mechanism
	s.startMessageRetry()

	log.Printf("MQTT server started on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// Stop stops the MQTT server
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
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
	clientID := packet.ClientID
	if clientID == "" {
		// Auto-generate client ID if not provided
		clientID = fmt.Sprintf("client-%s", conn.RemoteAddr().String())
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
	log.Printf("Received PUBLISH: topic=%s, qos=%d, messageID=%d, payload=%s\n",
		packet.TopicName, packet.Qos, packet.MessageID, string(packet.Payload))

	// Find client ID from connection
	var clientID string
	var username string
	s.clientsMutex.RLock()
	for id, client := range s.clients {
		if client.Conn == conn {
			clientID = id
			username = client.Username
			break
		}
	}
	s.clientsMutex.RUnlock()

	// Check publish permission
	if err := s.checkTopicPermission(clientID, packet.TopicName, true); err != nil {
		log.Printf("Permission denied for client %s to publish to topic %s: %v",
			clientID, packet.TopicName, err)
		// We can't send an error response for PUBLISH in MQTT 3.1.1
		// The server just drops the message silently
		return
	}

	// Store message in persistent storage
	s.persistMessage(clientID, packet.TopicName, packet.Payload, packet.Qos, packet.Retain)

	// Handle retained messages
	if packet.Retain {
		s.storeRetainedMessage(packet.TopicName, packet.Payload, packet.Qos)
	}

	// Trigger plugin event for message publishing
	s.triggerPluginEvent("message.publish", map[string]any{
		"ClientID":  clientID,
		"Username":  username,
		"Topic":     packet.TopicName,
		"Payload":   packet.Payload,
		"QoS":       packet.Qos,
		"Retained":  packet.Retain,
		"Timestamp": time.Now().Unix(),
	})

	// For QoS2, store the message until we receive PUBREL
	if packet.Qos == QoS2 {
		s.pendingQoS2Mutex.Lock()
		s.pendingQoS2Messages[packet.MessageID] = &PendingQoS2Message{
			MessageID:    packet.MessageID,
			ClientID:     clientID,
			Topic:        packet.TopicName,
			Payload:      packet.Payload,
			ReceivedTime: time.Now(),
		}
		s.pendingQoS2Mutex.Unlock()

		// Send PUBREC
		pubrec := NewPubRecPacket(packet.MessageID)
		pubrecBytes, err := pubrec.Encode()
		if err != nil {
			log.Printf("Error encoding PUBREC: %v\n", err)
			return
		}

		_, err = conn.Write(pubrecBytes)
		if err != nil {
			log.Printf("Error sending PUBREC: %v\n", err)
		}

		// For QoS2, we don't distribute the message until we get PUBREL
		return
	}

	// Distribute the message to subscribers for QoS0 and QoS1
	s.distributeMessage(packet.TopicName, packet.Payload, packet.Qos)

	// If QoS1, send PUBACK
	if packet.Qos == QoS1 {
		puback := NewPubAckPacket(packet.MessageID)
		pubackBytes, err := puback.Encode()
		if err != nil {
			log.Printf("Error encoding PUBACK: %v\n", err)
			return
		}

		_, err = conn.Write(pubackBytes)
		if err != nil {
			log.Printf("Error sending PUBACK: %v\n", err)
		}
	}
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
		log.Printf("Unknown client tried to subscribe\n")
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

	// Find matching subscriptions using topic matching
	for subTopic, subscriptions := range s.subscriptions {
		if topicMatches(subTopic, topic) {
			for _, subscription := range subscriptions {
				// Find the client
				s.clientsMutex.RLock()
				client, ok := s.clients[subscription.ClientID]
				s.clientsMutex.RUnlock()

				if ok && client.IsConnected {
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
						continue
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
			}
		}
	}
}

// generateMessageID generates a unique message ID for a client
func (s *Server) generateMessageID(clientID string) uint16 {
	// Simple implementation - should be improved for production
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()

	// Make sure the client map exists
	if _, ok := s.inflightMessages[clientID]; !ok {
		s.inflightMessages[clientID] = make(map[uint16]*InflightMessage)
	}

	// Generate a unique message ID (1-65535)
	var messageID uint16 = 1
	for {
		if _, exists := s.inflightMessages[clientID][messageID]; !exists {
			break
		}
		messageID++
		if messageID == 0 { // Avoid messageID 0 (wrap-around)
			messageID = 1
		}
	}

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

// storeRetainedMessage stores or deletes a retained message
func (s *Server) storeRetainedMessage(topic string, payload []byte, qos byte) {
	s.retainedMessagesMutex.Lock()
	defer s.retainedMessagesMutex.Unlock()

	if len(payload) == 0 {
		// Empty payload means delete the retained message
		delete(s.retainedMessages, topic)
		log.Printf("Deleted retained message for topic: %s", topic)
	} else {
		// Store the retained message
		s.retainedMessages[topic] = RetainedMessage{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			Modified: time.Now(),
		}
		log.Printf("Stored retained message for topic: %s", topic)
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
