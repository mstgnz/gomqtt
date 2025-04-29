package mqtt

import (
	"bufio"
	"fmt"
	"log"
	"net"
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
}

// RetainedMessage represents a message that should be stored and sent to new subscribers
type RetainedMessage struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Modified time.Time
}

// NewServer creates a new MQTT broker server instance
func NewServer(host string, port int) *Server {
	return &Server{
		Host:             host,
		Port:             port,
		clients:          make(map[string]*Client),
		subscriptions:    make(map[string][]*Subscription),
		retainedMessages: make(map[string]RetainedMessage),
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

				// Publish the Will message if present
				if clientToRemove.WillTopic != "" {
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
			s.clientsMutex.RLock()
			for _, client := range s.clients {
				if client.Conn == conn {
					// Graceful disconnect - don't trigger the Will message
					client.IsConnected = false
					break
				}
			}
			s.clientsMutex.RUnlock()

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
}

// handlePublish processes a PUBLISH packet
func (s *Server) handlePublish(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBLISH: topic=%s, qos=%d, payload=%s\n",
		packet.TopicName, packet.Qos, string(packet.Payload))

	// Handle retained messages
	if packet.Retain {
		s.retainedMessagesMutex.Lock()
		if len(packet.Payload) == 0 {
			// Empty payload means delete the retained message
			delete(s.retainedMessages, packet.TopicName)
			log.Printf("Deleted retained message for topic: %s", packet.TopicName)
		} else {
			// Store the retained message
			s.retainedMessages[packet.TopicName] = RetainedMessage{
				Topic:    packet.TopicName,
				Payload:  packet.Payload,
				QoS:      packet.Qos,
				Modified: time.Now(),
			}
			log.Printf("Stored retained message for topic: %s", packet.TopicName)
		}
		s.retainedMessagesMutex.Unlock()
	}

	// Distribute the message to subscribers
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
	} else if packet.Qos == QoS2 {
		// For QoS2, we need to implement the full QoS2 flow:
		// PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
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
		// Note: The rest of the QoS2 flow will be handled as we receive the PUBREL
	}
}

// handlePubAck processes a PUBACK packet (QoS 1 acknowledgment)
func (s *Server) handlePubAck(conn net.Conn, packet *Packet) {
	// For QoS 1, this is the end of the flow
	log.Printf("Received PUBACK for message ID %d", packet.MessageID)

	// In a more complete implementation, we would remove the message from our delivery queue
}

// handlePubRec processes a PUBREC packet (first acknowledgment in QoS 2 flow)
func (s *Server) handlePubRec(conn net.Conn, packet *Packet) {
	log.Printf("Received PUBREC for message ID %d", packet.MessageID)

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

	// At this point, we could deliver the QoS 2 message to subscribers
	// In a real implementation, we would have stored the message when we received the PUBLISH
}

// handlePubComp processes a PUBCOMP packet (final acknowledgment in QoS 2 flow)
func (s *Server) handlePubComp(conn net.Conn, packet *Packet) {
	// For QoS 2, this is the end of the flow
	log.Printf("Received PUBCOMP for message ID %d", packet.MessageID)

	// In a more complete implementation, we would remove the message from our delivery queue
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

	// Find matching subscriptions using simple topic matching
	// In a full implementation, this would handle wildcards (+ and #)
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

					// Create PUBLISH packet
					publish := NewPublishPacket(topic, payload, effectiveQoS, 1, false, false)
					publishBytes, err := publish.Encode()
					if err != nil {
						log.Printf("Error encoding PUBLISH for client %s: %v\n",
							subscription.ClientID, err)
						continue
					}

					// Send to client
					_, err = client.Conn.Write(publishBytes)
					if err != nil {
						log.Printf("Error sending PUBLISH to client %s: %v\n",
							subscription.ClientID, err)
					}
				}
			}
		}
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
	for i := 0; i < len(subSegments); i++ {
		// + wildcard matches any single segment
		if subSegments[i] == "+" {
			continue
		}

		// If segments don't match, no match
		if subSegments[i] != pubSegments[i] {
			return false
		}
	}

	// All segments matched
	return true
}
