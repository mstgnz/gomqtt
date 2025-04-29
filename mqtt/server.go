package mqtt

import (
	"fmt"
	"net"
	"sync"
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
}

// NewServer creates a new MQTT broker server instance
func NewServer(host string, port int) *Server {
	return &Server{
		Host:          host,
		Port:          port,
		clients:       make(map[string]*Client),
		subscriptions: make(map[string][]*Subscription),
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

	fmt.Printf("MQTT server started on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
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
	fmt.Printf("New connection from %s\n", conn.RemoteAddr().String())
	defer conn.Close()

	// Buffer for reading packet
	buffer := make([]byte, 4096)

	for {
		// Read fixed header
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Connection closed from %s: %v\n", conn.RemoteAddr().String(), err)
			break
		}

		if n == 0 {
			continue
		}

		// Parse packet type from first byte
		packetType := buffer[0] >> 4

		// Process packet based on its type
		switch packetType {
		case CONNECT:
			s.handleConnect(conn, buffer[:n])
		case PUBLISH:
			s.handlePublish(conn, buffer[:n])
		case SUBSCRIBE:
			s.handleSubscribe(conn, buffer[:n])
		case PINGREQ:
			s.handlePing(conn)
		case DISCONNECT:
			fmt.Printf("Client %s disconnected\n", conn.RemoteAddr().String())
			return
		default:
			fmt.Printf("Unsupported packet type: %d\n", packetType)
		}
	}
}

// handleConnect processes a CONNECT packet
func (s *Server) handleConnect(conn net.Conn, data []byte) {
	// TODO: Properly parse the CONNECT packet
	// For now, just send a CONNACK packet back

	// Create CONNACK packet (simple version for now)
	connack := []byte{
		CONNACK << 4, // Packet type
		2,            // Remaining length
		0,            // Connect acknowledge flags
		0,            // Return code 0 = connection accepted
	}

	_, err := conn.Write(connack)
	if err != nil {
		fmt.Printf("Error sending CONNACK: %v\n", err)
	} else {
		// Extract client ID (simplified for now)
		clientID := fmt.Sprintf("client-%s", conn.RemoteAddr().String())

		// Create and store client
		client := NewClient(clientID, conn)

		s.clientsMutex.Lock()
		s.clients[clientID] = client
		s.clientsMutex.Unlock()

		fmt.Printf("Client %s connected\n", clientID)
	}
}

// handlePublish processes a PUBLISH packet
func (s *Server) handlePublish(conn net.Conn, data []byte) {
	// TODO: Properly parse the PUBLISH packet
	// For now, we'll implement a simplified version

	// Skip fixed header
	remainingLength := int(data[1])
	currentPos := 2

	// Read topic length (2 bytes)
	topicLength := int(data[currentPos])<<8 | int(data[currentPos+1])
	currentPos += 2

	// Read topic
	topic := string(data[currentPos : currentPos+topicLength])
	currentPos += topicLength

	// Read message ID if QoS > 0
	var messageID uint16
	qos := (data[0] & 0x06) >> 1
	if qos > 0 {
		messageID = uint16(data[currentPos])<<8 | uint16(data[currentPos+1])
		currentPos += 2
	}

	// Read payload
	payload := data[currentPos : remainingLength+2]

	fmt.Printf("Received PUBLISH: topic=%s, qos=%d, payload=%s\n", topic, qos, string(payload))

	// Distribute the message to subscribers
	s.distributeMessage(topic, payload, qos)

	// Send PUBACK if QoS1
	if qos == 1 {
		puback := []byte{
			PUBACK << 4,            // Packet type
			2,                      // Remaining length
			byte(messageID >> 8),   // Message ID MSB
			byte(messageID & 0xFF), // Message ID LSB
		}

		_, err := conn.Write(puback)
		if err != nil {
			fmt.Printf("Error sending PUBACK: %v\n", err)
		}
	}
}

// handleSubscribe processes a SUBSCRIBE packet
func (s *Server) handleSubscribe(conn net.Conn, data []byte) {
	// TODO: Properly parse the SUBSCRIBE packet
	// For now, we'll implement a simplified version

	// Skip fixed header
	remainingLength := int(data[1])
	currentPos := 2

	// Read message ID
	messageID := uint16(data[currentPos])<<8 | uint16(data[currentPos+1])
	currentPos += 2

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
		fmt.Printf("Unknown client tried to subscribe\n")
		return
	}

	// Process subscription requests
	var grantedQoS []byte

	for currentPos < remainingLength {
		// Read topic length
		topicLength := int(data[currentPos])<<8 | int(data[currentPos+1])
		currentPos += 2

		// Read topic
		topic := string(data[currentPos : currentPos+topicLength])
		currentPos += topicLength

		// Read requested QoS
		qos := data[currentPos] & 0x03
		currentPos++

		fmt.Printf("Client %s subscribing to %s with QoS %d\n", clientID, topic, qos)

		// Store subscription
		subscription := client.Subscribe(topic, qos)

		// Add to server's subscription map
		s.subMutex.Lock()
		s.subscriptions[topic] = append(s.subscriptions[topic], subscription)
		s.subMutex.Unlock()

		// Grant requested QoS
		grantedQoS = append(grantedQoS, qos)
	}

	// Send SUBACK
	suback := make([]byte, 2+1+len(grantedQoS))
	suback[0] = SUBACK << 4
	suback[1] = byte(len(grantedQoS) + 2) // Remaining length
	suback[2] = byte(messageID >> 8)      // Message ID MSB
	suback[3] = byte(messageID & 0xFF)    // Message ID LSB

	// Add granted QoS values
	for i, qos := range grantedQoS {
		suback[4+i] = qos
	}

	_, err := conn.Write(suback)
	if err != nil {
		fmt.Printf("Error sending SUBACK: %v\n", err)
	}
}

// handlePing responds to a PINGREQ with a PINGRESP
func (s *Server) handlePing(conn net.Conn) {
	pingResp := []byte{
		PINGRESP << 4, // Packet type
		0,             // Remaining length
	}

	_, err := conn.Write(pingResp)
	if err != nil {
		fmt.Printf("Error sending PINGRESP: %v\n", err)
	}
}

// distributeMessage sends a published message to all relevant subscribers
func (s *Server) distributeMessage(topic string, payload []byte, qos byte) {
	s.subMutex.RLock()
	defer s.subMutex.RUnlock()

	// Find matching subscriptions using simple topic matching
	// In a full implementation, this would handle wildcards (+ and #)
	for subTopic, subscriptions := range s.subscriptions {
		if subTopic == topic {
			for _, subscription := range subscriptions {
				// Find the client
				s.clientsMutex.RLock()
				client, ok := s.clients[subscription.ClientID]
				s.clientsMutex.RUnlock()

				if ok && client.IsConnected {
					// Create PUBLISH packet
					publish := createPublishPacket(topic, payload, subscription.QoS)

					// Send to client
					_, err := client.Conn.Write(publish)
					if err != nil {
						fmt.Printf("Error sending PUBLISH to client %s: %v\n", subscription.ClientID, err)
					}
				}
			}
		}
	}
}

// createPublishPacket creates a PUBLISH packet
func createPublishPacket(topic string, payload []byte, qos byte) []byte {
	topicLen := len(topic)
	remainingLength := 2 + topicLen + len(payload)

	// Add space for message ID if QoS > 0
	if qos > 0 {
		remainingLength += 2
	}

	// Create the packet
	packet := make([]byte, 1+1+remainingLength) // Fixed header (2) + variable header + payload

	// Fixed header
	packet[0] = PUBLISH<<4 | (qos << 1) // Packet type + QoS
	packet[1] = byte(remainingLength)   // Remaining length

	// Variable header - Topic name
	packet[2] = byte(topicLen >> 8)   // Topic length MSB
	packet[3] = byte(topicLen & 0xFF) // Topic length LSB

	// Copy topic
	copy(packet[4:], []byte(topic))

	// Payload position
	payloadPos := 4 + topicLen

	// Add message ID if QoS > 0
	if qos > 0 {
		// For now, use a fixed message ID = 1
		packet[payloadPos] = 0
		packet[payloadPos+1] = 1
		payloadPos += 2
	}

	// Copy payload
	copy(packet[payloadPos:], payload)

	return packet
}
