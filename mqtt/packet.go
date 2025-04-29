package mqtt

// MQTT packet types
const (
	CONNECT     = 1
	CONNACK     = 2
	PUBLISH     = 3
	PUBACK      = 4
	PUBREC      = 5
	PUBREL      = 6
	PUBCOMP     = 7
	SUBSCRIBE   = 8
	SUBACK      = 9
	UNSUBSCRIBE = 10
	UNSUBACK    = 11
	PINGREQ     = 12
	PINGRESP    = 13
	DISCONNECT  = 14
)

// QoS levels
const (
	QoS0 = 0 // At most once
	QoS1 = 1 // At least once
	QoS2 = 2 // Exactly once
)

// Packet represents an MQTT control packet
type Packet struct {
	// Fixed header
	PacketType byte
	Dup        bool
	Qos        byte
	Retain     bool

	// Variable header
	ProtocolName    string
	ProtocolVersion byte
	ConnectFlags    byte
	KeepAlive       uint16

	// Payload
	ClientID     string
	Username     string
	Password     []byte
	WillTopic    string
	WillMessage  []byte
	WillQoS      byte
	WillRetain   bool
	CleanSession bool

	// For PUBLISH
	TopicName string
	MessageID uint16
	Payload   []byte

	// For SUBSCRIBE
	Topics []string
	QoSs   []byte
}

// NewConnectPacket creates a new CONNECT packet
func NewConnectPacket(clientID string, username string, password []byte) *Packet {
	return &Packet{
		PacketType:      CONNECT,
		ProtocolName:    "MQTT",
		ProtocolVersion: 4, // MQTT 3.1.1
		ClientID:        clientID,
		Username:        username,
		Password:        password,
		CleanSession:    true,
		KeepAlive:       60, // 60 seconds
	}
}

// NewPublishPacket creates a new PUBLISH packet
func NewPublishPacket(topic string, payload []byte, qos byte, retain bool) *Packet {
	return &Packet{
		PacketType: PUBLISH,
		TopicName:  topic,
		Payload:    payload,
		Qos:        qos,
		Retain:     retain,
	}
}

// NewSubscribePacket creates a new SUBSCRIBE packet
func NewSubscribePacket(topics []string, qos []byte, messageID uint16) *Packet {
	return &Packet{
		PacketType: SUBSCRIBE,
		Topics:     topics,
		QoSs:       qos,
		MessageID:  messageID,
	}
}
