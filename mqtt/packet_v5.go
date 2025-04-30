package mqtt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MQTT v5.0 packet type constants
const (
	// MQTT v5.0 specific packet type
	AUTH = 15
)

// Protocol versions
const (
	MQTT_3_1_1 = 4
	MQTT_5_0   = 5
)

// MQTT v5.0 Reason Codes
const (
	// Success codes
	SUCCESS       = 0x00
	GRANTED_QOS_0 = 0x00
	GRANTED_QOS_1 = 0x01
	GRANTED_QOS_2 = 0x02

	// Disconnect reason codes
	NORMAL_DISCONNECTION                   = 0x00
	DISCONNECT_WITH_WILL_MESSAGE           = 0x04
	UNSPECIFIED_ERROR                      = 0x80
	MALFORMED_PACKET                       = 0x81
	PROTOCOL_ERROR                         = 0x82
	IMPLEMENTATION_SPECIFIC_ERROR          = 0x83
	UNSUPPORTED_PROTOCOL_VERSION           = 0x84
	CLIENT_IDENTIFIER_NOT_VALID            = 0x85
	BAD_USERNAME_OR_PASSWORD               = 0x86
	NOT_AUTHORIZED                         = 0x87
	SERVER_UNAVAILABLE                     = 0x88
	SERVER_BUSY                            = 0x89
	BANNED                                 = 0x8A
	SERVER_SHUTTING_DOWN                   = 0x8B
	BAD_AUTHENTICATION_METHOD              = 0x8C
	KEEP_ALIVE_TIMEOUT                     = 0x8D
	SESSION_TAKEN_OVER                     = 0x8E
	TOPIC_FILTER_INVALID                   = 0x8F
	TOPIC_NAME_INVALID                     = 0x90
	PACKET_IDENTIFIER_IN_USE               = 0x91
	PACKET_IDENTIFIER_NOT_FOUND            = 0x92
	RECEIVE_MAXIMUM_EXCEEDED               = 0x93
	TOPIC_ALIAS_INVALID                    = 0x94
	PACKET_TOO_LARGE                       = 0x95
	MESSAGE_RATE_TOO_HIGH                  = 0x96
	QUOTA_EXCEEDED                         = 0x97
	ADMINISTRATIVE_ACTION                  = 0x98
	PAYLOAD_FORMAT_INVALID                 = 0x99
	RETAIN_NOT_SUPPORTED                   = 0x9A
	QOS_NOT_SUPPORTED                      = 0x9B
	USE_ANOTHER_SERVER                     = 0x9C
	SERVER_MOVED                           = 0x9D
	SHARED_SUBSCRIPTIONS_NOT_SUPPORTED     = 0x9E
	CONNECTION_RATE_EXCEEDED               = 0x9F
	MAXIMUM_CONNECT_TIME                   = 0xA0
	SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xA1
	WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED   = 0xA2
)

// MQTT v5.0 Property Identifiers
const (
	PAYLOAD_FORMAT_INDICATOR          = 0x01
	MESSAGE_EXPIRY_INTERVAL           = 0x02
	CONTENT_TYPE                      = 0x03
	RESPONSE_TOPIC                    = 0x08
	CORRELATION_DATA                  = 0x09
	SUBSCRIPTION_IDENTIFIER           = 0x0B
	SESSION_EXPIRY_INTERVAL           = 0x11
	ASSIGNED_CLIENT_IDENTIFIER        = 0x12
	SERVER_KEEP_ALIVE                 = 0x13
	AUTHENTICATION_METHOD             = 0x15
	AUTHENTICATION_DATA               = 0x16
	REQUEST_PROBLEM_INFORMATION       = 0x17
	WILL_DELAY_INTERVAL               = 0x18
	REQUEST_RESPONSE_INFORMATION      = 0x19
	RESPONSE_INFORMATION              = 0x1A
	SERVER_REFERENCE                  = 0x1C
	REASON_STRING                     = 0x1F
	RECEIVE_MAXIMUM                   = 0x21
	TOPIC_ALIAS_MAXIMUM               = 0x22
	TOPIC_ALIAS                       = 0x23
	MAXIMUM_QOS                       = 0x24
	RETAIN_AVAILABLE                  = 0x25
	USER_PROPERTY                     = 0x26
	MAXIMUM_PACKET_SIZE               = 0x27
	WILDCARD_SUBSCRIPTION_AVAILABLE   = 0x28
	SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29
	SHARED_SUBSCRIPTION_AVAILABLE     = 0x2A
)

// PropertyValue represents a property with its value
type PropertyValue struct {
	PropertyID byte
	Value      interface{}
}

// PacketV5 represents an MQTT v5.0 control packet
type PacketV5 struct {
	// Fixed header (same as v3.1.1)
	PacketType byte
	Dup        bool
	Qos        byte
	Retain     bool

	// Variable header fields
	ProtocolName    string
	ProtocolVersion byte
	ConnectFlags    byte
	KeepAlive       uint16
	MessageID       uint16
	ReasonCode      byte

	// Properties
	Properties []PropertyValue

	// Payload fields
	ClientID    string
	Username    string
	Password    []byte
	WillTopic   string
	WillMessage []byte
	WillQoS     byte
	WillRetain  bool
	CleanStart  bool // In MQTT 5.0, "Clean Session" flag is renamed to "Clean Start"

	// For PUBLISH
	TopicName string
	Payload   []byte

	// For SUBSCRIBE/UNSUBSCRIBE
	Topics            []string
	QoSs              []byte
	NoLocal           []bool
	RetainAsPublished []bool
	RetainHandling    []byte

	// For user properties
	UserProperties map[string][]string

	// Raw packet for easy retrieval
	rawBytes []byte
}

// NewConnectPacketV5 creates a new CONNECT packet for MQTT v5.0
func NewConnectPacketV5(clientID string, username string, password []byte, cleanStart bool) *PacketV5 {
	packet := &PacketV5{
		PacketType:      CONNECT,
		ProtocolName:    "MQTT",
		ProtocolVersion: MQTT_5_0,
		ClientID:        clientID,
		CleanStart:      cleanStart,
		KeepAlive:       60, // 60 seconds default
		Properties:      make([]PropertyValue, 0),
		UserProperties:  make(map[string][]string),
	}

	// Set username and password if provided
	if username != "" {
		packet.Username = username
		if password != nil {
			packet.Password = password
		}
	}

	return packet
}

// AddProperty adds a property to the packet
func (p *PacketV5) AddProperty(propertyID byte, value interface{}) error {
	p.Properties = append(p.Properties, PropertyValue{PropertyID: propertyID, Value: value})
	return nil
}

// AddUserProperty adds a user property to the packet
func (p *PacketV5) AddUserProperty(name, value string) {
	if p.UserProperties == nil {
		p.UserProperties = make(map[string][]string)
	}

	p.UserProperties[name] = append(p.UserProperties[name], value)
}

// NewPublishPacketV5 creates a new PUBLISH packet for MQTT v5.0
func NewPublishPacketV5(topic string, payload []byte, qos byte, messageID uint16, dup bool, retain bool) *PacketV5 {
	return &PacketV5{
		PacketType:     PUBLISH,
		TopicName:      topic,
		Payload:        payload,
		Qos:            qos,
		MessageID:      messageID,
		Dup:            dup,
		Retain:         retain,
		Properties:     make([]PropertyValue, 0),
		UserProperties: make(map[string][]string),
	}
}

// NewSubscribePacketV5 creates a new SUBSCRIBE packet for MQTT v5.0
func NewSubscribePacketV5(topics []string, qos []byte, messageID uint16) *PacketV5 {
	return &PacketV5{
		PacketType:        SUBSCRIBE,
		Topics:            topics,
		QoSs:              qos,
		MessageID:         messageID,
		NoLocal:           make([]bool, len(topics)),
		RetainAsPublished: make([]bool, len(topics)),
		RetainHandling:    make([]byte, len(topics)),
		Properties:        make([]PropertyValue, 0),
		UserProperties:    make(map[string][]string),
	}
}

// NewDisconnectPacketV5 creates a new DISCONNECT packet with reason code
func NewDisconnectPacketV5(reasonCode byte) *PacketV5 {
	return &PacketV5{
		PacketType:     DISCONNECT,
		ReasonCode:     reasonCode,
		Properties:     make([]PropertyValue, 0),
		UserProperties: make(map[string][]string),
	}
}

// NewAuthPacketV5 creates a new AUTH packet
func NewAuthPacketV5(reasonCode byte) *PacketV5 {
	return &PacketV5{
		PacketType:     AUTH,
		ReasonCode:     reasonCode,
		Properties:     make([]PropertyValue, 0),
		UserProperties: make(map[string][]string),
	}
}

// Additional methods to encode properties for V5 packets
func encodeProperties(buf *bytes.Buffer, properties []PropertyValue, userProperties map[string][]string) error {
	propBuf := new(bytes.Buffer)

	// Encode all regular properties
	for _, prop := range properties {
		propBuf.WriteByte(prop.PropertyID)

		switch prop.PropertyID {
		// Byte properties
		case PAYLOAD_FORMAT_INDICATOR, REQUEST_PROBLEM_INFORMATION,
			REQUEST_RESPONSE_INFORMATION, MAXIMUM_QOS, RETAIN_AVAILABLE,
			WILDCARD_SUBSCRIPTION_AVAILABLE, SUBSCRIPTION_IDENTIFIER_AVAILABLE,
			SHARED_SUBSCRIPTION_AVAILABLE:
			propBuf.WriteByte(prop.Value.(byte))

		// 2-byte integer properties
		case SERVER_KEEP_ALIVE, RECEIVE_MAXIMUM, TOPIC_ALIAS_MAXIMUM, TOPIC_ALIAS:
			binary.Write(propBuf, binary.BigEndian, uint16(prop.Value.(int)))

		// 4-byte integer properties
		case MESSAGE_EXPIRY_INTERVAL, SESSION_EXPIRY_INTERVAL, WILL_DELAY_INTERVAL,
			MAXIMUM_PACKET_SIZE:
			binary.Write(propBuf, binary.BigEndian, uint32(prop.Value.(int)))

		// Variable byte integer properties
		case SUBSCRIPTION_IDENTIFIER:
			encodeVarByteInt(propBuf, prop.Value.(int))

		// String properties
		case CONTENT_TYPE, RESPONSE_TOPIC, ASSIGNED_CLIENT_IDENTIFIER,
			AUTHENTICATION_METHOD, RESPONSE_INFORMATION, SERVER_REFERENCE, REASON_STRING:
			encodeString(propBuf, prop.Value.(string))

		// Binary data properties
		case CORRELATION_DATA, AUTHENTICATION_DATA:
			encodeBytes(propBuf, prop.Value.([]byte))
		}
	}

	// Encode user properties
	for name, values := range userProperties {
		for _, value := range values {
			propBuf.WriteByte(USER_PROPERTY)
			encodeString(propBuf, name)
			encodeString(propBuf, value)
		}
	}

	// Write property length
	propLength := propBuf.Len()
	encodeVarByteInt(buf, propLength)

	// Write properties
	if propLength > 0 {
		buf.Write(propBuf.Bytes())
	}

	return nil
}

// encodeVarByteInt encodes a variable byte integer
func encodeVarByteInt(buf *bytes.Buffer, value int) {
	for {
		encodedByte := byte(value % 128)
		value = value / 128
		if value > 0 {
			encodedByte |= 128
		}
		buf.WriteByte(encodedByte)
		if value == 0 {
			break
		}
	}
}

// readVarByteInt reads a variable byte integer
func readVarByteInt(data []byte, index *int) (int, error) {
	multiplier := 1
	value := 0

	for {
		if *index >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}

		encodedByte := data[*index]
		*index++

		value += int(encodedByte&127) * multiplier
		multiplier *= 128

		if multiplier > 128*128*128 {
			return 0, errors.New("variable byte integer too large")
		}

		if (encodedByte & 128) == 0 {
			break
		}
	}

	return value, nil
}

// decodeProperties reads and decodes property fields
func decodeProperties(data []byte, index *int) ([]PropertyValue, map[string][]string, error) {
	properties := make([]PropertyValue, 0)
	userProperties := make(map[string][]string)

	// Read property length
	propLength, err := readVarByteInt(data, index)
	if err != nil {
		return nil, nil, err
	}

	// No properties
	if propLength == 0 {
		return properties, userProperties, nil
	}

	// Read properties
	endIndex := *index + propLength
	for *index < endIndex {
		if *index >= len(data) {
			return nil, nil, io.ErrUnexpectedEOF
		}

		propertyID := data[*index]
		*index++

		switch propertyID {
		// Byte properties
		case PAYLOAD_FORMAT_INDICATOR, REQUEST_PROBLEM_INFORMATION,
			REQUEST_RESPONSE_INFORMATION, MAXIMUM_QOS, RETAIN_AVAILABLE,
			WILDCARD_SUBSCRIPTION_AVAILABLE, SUBSCRIPTION_IDENTIFIER_AVAILABLE,
			SHARED_SUBSCRIPTION_AVAILABLE:
			if *index >= len(data) {
				return nil, nil, io.ErrUnexpectedEOF
			}
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: data[*index]})
			*index++

		// 2-byte integer properties
		case SERVER_KEEP_ALIVE, RECEIVE_MAXIMUM, TOPIC_ALIAS_MAXIMUM, TOPIC_ALIAS:
			if *index+2 > len(data) {
				return nil, nil, io.ErrUnexpectedEOF
			}
			val := int(binary.BigEndian.Uint16(data[*index : *index+2]))
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: val})
			*index += 2

		// 4-byte integer properties
		case MESSAGE_EXPIRY_INTERVAL, SESSION_EXPIRY_INTERVAL, WILL_DELAY_INTERVAL,
			MAXIMUM_PACKET_SIZE:
			if *index+4 > len(data) {
				return nil, nil, io.ErrUnexpectedEOF
			}
			val := int(binary.BigEndian.Uint32(data[*index : *index+4]))
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: val})
			*index += 4

		// Variable byte integer properties
		case SUBSCRIPTION_IDENTIFIER:
			val, err := readVarByteInt(data, index)
			if err != nil {
				return nil, nil, err
			}
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: val})

		// String properties
		case CONTENT_TYPE, RESPONSE_TOPIC, ASSIGNED_CLIENT_IDENTIFIER,
			AUTHENTICATION_METHOD, RESPONSE_INFORMATION, SERVER_REFERENCE, REASON_STRING:
			val, err := readString(data, index)
			if err != nil {
				return nil, nil, err
			}
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: val})

		// Binary data properties
		case CORRELATION_DATA, AUTHENTICATION_DATA:
			val, err := readBytes(data, index)
			if err != nil {
				return nil, nil, err
			}
			properties = append(properties, PropertyValue{PropertyID: propertyID, Value: val})

		// User property
		case USER_PROPERTY:
			name, err := readString(data, index)
			if err != nil {
				return nil, nil, err
			}
			value, err := readString(data, index)
			if err != nil {
				return nil, nil, err
			}
			userProperties[name] = append(userProperties[name], value)

		default:
			return nil, nil, fmt.Errorf("unknown property identifier: %d", propertyID)
		}
	}

	// Validate that we read all properties correctly
	if *index != endIndex {
		return nil, nil, errors.New("property length mismatch")
	}

	return properties, userProperties, nil
}

// EncodeV5 encodes an MQTT v5.0 packet
func (p *PacketV5) EncodeV5() ([]byte, error) {
	var buf bytes.Buffer

	// Encode variable header and payload, keeping track of size
	var variableHeaderAndPayload bytes.Buffer

	switch p.PacketType {
	case CONNECT:
		if err := p.encodeConnectV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case CONNACK:
		if err := p.encodeConnAckV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case PUBLISH:
		if err := p.encodePublishV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		if err := p.encodePubAckV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case SUBSCRIBE:
		if err := p.encodeSubscribeV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case SUBACK:
		if err := p.encodeSubAckV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case UNSUBSCRIBE:
		if err := p.encodeUnsubscribeV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case UNSUBACK:
		if err := p.encodeUnsubAckV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case DISCONNECT:
		if err := p.encodeDisconnectV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case AUTH:
		if err := p.encodeAuthV5(&variableHeaderAndPayload); err != nil {
			return nil, err
		}
	case PINGREQ, PINGRESP:
		// No variable header or payload for these
	default:
		return nil, fmt.Errorf("unknown packet type: %d", p.PacketType)
	}

	// Encode fixed header
	var fixedHeaderByte byte = p.PacketType << 4

	// Set appropriate flags in fixed header
	if p.PacketType == PUBLISH {
		if p.Dup {
			fixedHeaderByte |= 0x08
		}
		fixedHeaderByte |= (p.Qos << 1) & 0x06
		if p.Retain {
			fixedHeaderByte |= 0x01
		}
	} else if p.PacketType == PUBREL || p.PacketType == SUBSCRIBE || p.PacketType == UNSUBSCRIBE {
		fixedHeaderByte |= 0x02 // These packet types set reserved bit to 1
	}

	buf.WriteByte(fixedHeaderByte)

	// Write remaining length
	remainingLength := variableHeaderAndPayload.Len()
	remainingLengthBytes := encodeRemainingLength(remainingLength)
	buf.Write(remainingLengthBytes)

	// Write variable header and payload
	buf.Write(variableHeaderAndPayload.Bytes())

	return buf.Bytes(), nil
}

// encodeConnectV5 encodes the variable header and payload for CONNECT packets
func (p *PacketV5) encodeConnectV5(buf *bytes.Buffer) error {
	// Protocol name
	encodeString(buf, p.ProtocolName)

	// Protocol version
	buf.WriteByte(p.ProtocolVersion)

	// Connect flags
	var connectFlags byte
	if p.CleanStart {
		connectFlags |= 0x02
	}
	if p.WillTopic != "" {
		connectFlags |= 0x04 // Will flag
		connectFlags |= (p.WillQoS & 0x03) << 3
		if p.WillRetain {
			connectFlags |= 0x20
		}
	}
	if p.Username != "" {
		connectFlags |= 0x80
		if len(p.Password) > 0 {
			connectFlags |= 0x40
		}
	}
	buf.WriteByte(connectFlags)

	// Keep alive
	binary.Write(buf, binary.BigEndian, p.KeepAlive)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Payload - Client ID
	encodeString(buf, p.ClientID)

	// Will properties and Will topic & message if present
	if p.WillTopic != "" {
		// We would encode will properties here if implemented
		propBuf := new(bytes.Buffer)
		encodeVarByteInt(propBuf, 0) // No will properties for now
		buf.Write(propBuf.Bytes())

		encodeString(buf, p.WillTopic)
		encodeBytes(buf, p.WillMessage)
	}

	// Username
	if p.Username != "" {
		encodeString(buf, p.Username)
		if len(p.Password) > 0 {
			encodeBytes(buf, p.Password)
		}
	}

	return nil
}

// encodeConnAckV5 encodes the variable header for CONNACK packets
func (p *PacketV5) encodeConnAckV5(buf *bytes.Buffer) error {
	// Connack flags (bit 0 = session present)
	if p.ConnectFlags&0x01 > 0 {
		buf.WriteByte(0x01) // Session present
	} else {
		buf.WriteByte(0x00) // No session
	}

	// Return code (now called Reason Code in MQTT 5.0)
	buf.WriteByte(p.ReasonCode)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	return nil
}

// encodePublishV5 encodes the variable header and payload for PUBLISH packets
func (p *PacketV5) encodePublishV5(buf *bytes.Buffer) error {
	// Topic name
	encodeString(buf, p.TopicName)

	// Packet Identifier (only for QoS > 0)
	if p.Qos > 0 {
		binary.Write(buf, binary.BigEndian, p.MessageID)
	}

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Payload
	buf.Write(p.Payload)

	return nil
}

// encodePubAckV5 encodes variable header for PUBACK, PUBREC, PUBREL, and PUBCOMP packets
func (p *PacketV5) encodePubAckV5(buf *bytes.Buffer) error {
	// Packet Identifier
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Reason Code
	if p.ReasonCode != 0 || len(p.Properties) > 0 || len(p.UserProperties) > 0 {
		buf.WriteByte(p.ReasonCode)

		// Properties
		encodeProperties(buf, p.Properties, p.UserProperties)
	}

	return nil
}

// encodeSubscribeV5 encodes the variable header and payload for SUBSCRIBE packets
func (p *PacketV5) encodeSubscribeV5(buf *bytes.Buffer) error {
	// Packet Identifier
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Topic Filters
	for i, topic := range p.Topics {
		encodeString(buf, topic)

		// Subscription Options
		// Each byte of QoSs contains:
		// - Bits 0-1: QoS level
		// - Bit 2: No Local flag
		// - Bit 3: Retain As Published flag
		// - Bits 4-5: Retain Handling
		var options byte = p.QoSs[i] & 0x03 // QoS

		if i < len(p.NoLocal) && p.NoLocal[i] {
			options |= 0x04
		}

		if i < len(p.RetainAsPublished) && p.RetainAsPublished[i] {
			options |= 0x08
		}

		if i < len(p.RetainHandling) {
			options |= (p.RetainHandling[i] & 0x03) << 4
		}

		buf.WriteByte(options)
	}

	return nil
}

// encodeSubAckV5 encodes the variable header for SUBACK packets
func (p *PacketV5) encodeSubAckV5(buf *bytes.Buffer) error {
	// Packet Identifier
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Return Codes / Reason Codes
	for _, qos := range p.QoSs {
		buf.WriteByte(qos)
	}

	return nil
}

// encodeUnsubscribeV5 encodes the variable header and payload for UNSUBSCRIBE packets
func (p *PacketV5) encodeUnsubscribeV5(buf *bytes.Buffer) error {
	// Packet Identifier
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Topic Filters
	for _, topic := range p.Topics {
		encodeString(buf, topic)
	}

	return nil
}

// encodeUnsubAckV5 encodes the variable header for UNSUBACK packets
func (p *PacketV5) encodeUnsubAckV5(buf *bytes.Buffer) error {
	// Packet Identifier
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	// Reason Codes - if we have them
	if len(p.QoSs) > 0 {
		for _, code := range p.QoSs {
			buf.WriteByte(code)
		}
	}

	return nil
}

// encodeDisconnectV5 encodes the variable header for DISCONNECT packets
func (p *PacketV5) encodeDisconnectV5(buf *bytes.Buffer) error {
	// Reason Code
	buf.WriteByte(p.ReasonCode)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	return nil
}

// encodeAuthV5 encodes the variable header for AUTH packets
func (p *PacketV5) encodeAuthV5(buf *bytes.Buffer) error {
	// Reason Code
	buf.WriteByte(p.ReasonCode)

	// Properties
	encodeProperties(buf, p.Properties, p.UserProperties)

	return nil
}

// ReadPacketV5 reads an MQTT v5.0 packet from a reader
func ReadPacketV5(reader io.Reader) (*PacketV5, error) {
	// Read the first byte (control header)
	var header [1]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}

	// Extract packet type and flags
	packetType := header[0] >> 4
	flags := header[0] & 0x0F

	// Read the remaining length
	remainingLength, err := readRemainingLength(reader)
	if err != nil {
		return nil, err
	}

	// Read the rest of the packet
	buf := make([]byte, remainingLength)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}

	// Create the packet structure
	packet := &PacketV5{
		PacketType: packetType,
		rawBytes:   append([]byte{header[0]}, buf...),
	}

	// Parse according to packet type
	switch packetType {
	case CONNECT:
		return parseConnectV5(packet, buf, flags)
	case CONNACK:
		return parseConnAckV5(packet, buf, flags)
	case PUBLISH:
		packet.Dup = (flags & 0x08) > 0
		packet.Qos = (flags & 0x06) >> 1
		packet.Retain = (flags & 0x01) > 0
		return parsePublishV5(packet, buf)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		return parsePubAckV5(packet, buf, flags, packetType)
	case SUBSCRIBE:
		return parseSubscribeV5(packet, buf, flags)
	case SUBACK:
		return parseSubAckV5(packet, buf, flags)
	case UNSUBSCRIBE:
		return parseUnsubscribeV5(packet, buf, flags)
	case UNSUBACK:
		return parseUnsubAckV5(packet, buf, flags)
	case DISCONNECT:
		return parseDisconnectV5(packet, buf, flags)
	case AUTH:
		return parseAuthV5(packet, buf, flags)
	case PINGREQ, PINGRESP:
		// These packets have no variable header or payload
		if len(buf) > 0 {
			return nil, ErrInvalidPacket
		}
		return packet, nil
	default:
		return nil, fmt.Errorf("unknown packet type: %d", packetType)
	}
}

// parseConnectV5 parses a CONNECT packet
func parseConnectV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 10 { // Minimum length for a Connect packet
		return nil, ErrInvalidPacket
	}

	index := 0

	// Protocol name
	protocolName, err := readString(data, &index)
	if err != nil {
		return nil, err
	}
	packet.ProtocolName = protocolName

	// Protocol version
	if index >= len(data) {
		return nil, ErrInvalidPacket
	}
	packet.ProtocolVersion = data[index]
	index++

	// Connect flags
	if index >= len(data) {
		return nil, ErrInvalidPacket
	}
	connectFlags := data[index]
	packet.ConnectFlags = connectFlags
	index++

	// Parse connect flags
	packet.CleanStart = (connectFlags & 0x02) > 0
	willFlag := (connectFlags & 0x04) > 0
	willQoS := (connectFlags & 0x18) >> 3
	willRetain := (connectFlags & 0x20) > 0
	passwordFlag := (connectFlags & 0x40) > 0
	usernameFlag := (connectFlags & 0x80) > 0

	// Keep alive
	if index+2 > len(data) {
		return nil, ErrInvalidPacket
	}
	packet.KeepAlive = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Properties
	properties, userProperties, err := decodeProperties(data, &index)
	if err != nil {
		return nil, err
	}
	packet.Properties = properties
	packet.UserProperties = userProperties

	// Client ID
	clientID, err := readString(data, &index)
	if err != nil {
		return nil, err
	}
	packet.ClientID = clientID

	// Will properties and Will topic & message
	if willFlag {
		// Skip will properties for now
		propLength, err := readVarByteInt(data, &index)
		if err != nil {
			return nil, err
		}
		index += propLength // Skip over will properties

		willTopic, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		packet.WillTopic = willTopic

		willMessage, err := readBytes(data, &index)
		if err != nil {
			return nil, err
		}
		packet.WillMessage = willMessage
		packet.WillQoS = byte(willQoS)
		packet.WillRetain = willRetain
	}

	// Username
	if usernameFlag {
		username, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		packet.Username = username
	}

	// Password
	if passwordFlag {
		password, err := readBytes(data, &index)
		if err != nil {
			return nil, err
		}
		packet.Password = password
	}

	return packet, nil
}

// parseConnAckV5 parses a CONNACK packet
func parseConnAckV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 2 {
		return nil, ErrInvalidPacket
	}

	index := 0

	// Connack flags (bit 0 = session present)
	packet.ConnectFlags = data[index] & 0x01 // Only save session present bit
	index++

	// Reason Code
	packet.ReasonCode = data[index]
	index++

	// Properties
	if index < len(data) {
		properties, userProperties, err := decodeProperties(data, &index)
		if err != nil {
			return nil, err
		}
		packet.Properties = properties
		packet.UserProperties = userProperties
	}

	return packet, nil
}

// parsePublishV5 parses a PUBLISH packet
func parsePublishV5(packet *PacketV5, data []byte) (*PacketV5, error) {
	if len(data) < 2 { // Minimum for topic length
		return nil, ErrInvalidPacket
	}

	index := 0

	// Topic name
	topicName, err := readString(data, &index)
	if err != nil {
		return nil, err
	}
	packet.TopicName = topicName

	// Message ID (only for QoS > 0)
	if packet.Qos > 0 {
		if index+2 > len(data) {
			return nil, ErrInvalidPacket
		}
		packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
		index += 2
	}

	// Properties
	if index < len(data) {
		properties, userProperties, err := decodeProperties(data, &index)
		if err != nil {
			return nil, err
		}
		packet.Properties = properties
		packet.UserProperties = userProperties
	}

	// Payload
	if index < len(data) {
		packet.Payload = data[index:]
	} else {
		packet.Payload = []byte{}
	}

	return packet, nil
}

// parsePubAckV5 parses PUBACK, PUBREC, PUBREL, and PUBCOMP packets
func parsePubAckV5(packet *PacketV5, data []byte, flags byte, packetType byte) (*PacketV5, error) {
	if len(data) < 2 {
		return nil, ErrInvalidPacket
	}

	index := 0

	// Packet Identifier
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Reason Code (if available)
	if index < len(data) {
		packet.ReasonCode = data[index]
		index++

		// Properties (if available)
		if index < len(data) {
			properties, userProperties, err := decodeProperties(data, &index)
			if err != nil {
				return nil, err
			}
			packet.Properties = properties
			packet.UserProperties = userProperties
		}
	} else {
		// If no reason code, assume success (0)
		packet.ReasonCode = 0
	}

	return packet, nil
}

// parseSubscribeV5 parses a SUBSCRIBE packet
func parseSubscribeV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Properties
	properties, userProperties, err := decodeProperties(data, &index)
	if err != nil {
		return nil, err
	}
	packet.Properties = properties
	packet.UserProperties = userProperties

	// Topic filters and options
	var topics []string
	var qos []byte
	var noLocal []bool
	var retainAsPublished []bool
	var retainHandling []byte

	for index < len(data) {
		// Topic filter
		topicFilter, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topicFilter)

		// Subscription Options
		if index >= len(data) {
			return nil, ErrInvalidPacket
		}

		options := data[index]
		index++

		// Bits 0-1: QoS
		qos = append(qos, options&0x03)

		// Bit 2: No Local
		noLocal = append(noLocal, (options&0x04) > 0)

		// Bit 3: Retain As Published
		retainAsPublished = append(retainAsPublished, (options&0x08) > 0)

		// Bits 4-5: Retain Handling
		retainHandling = append(retainHandling, (options&0x30)>>4)
	}

	packet.Topics = topics
	packet.QoSs = qos
	packet.NoLocal = noLocal
	packet.RetainAsPublished = retainAsPublished
	packet.RetainHandling = retainHandling

	return packet, nil
}

// parseSubAckV5 parses a SUBACK packet
func parseSubAckV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Properties
	properties, userProperties, err := decodeProperties(data, &index)
	if err != nil {
		return nil, err
	}
	packet.Properties = properties
	packet.UserProperties = userProperties

	// Reason Codes
	var qos []byte
	for index < len(data) {
		qos = append(qos, data[index])
		index++
	}

	packet.QoSs = qos

	return packet, nil
}

// parseUnsubscribeV5 parses an UNSUBSCRIBE packet
func parseUnsubscribeV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Properties
	properties, userProperties, err := decodeProperties(data, &index)
	if err != nil {
		return nil, err
	}
	packet.Properties = properties
	packet.UserProperties = userProperties

	// Topic filters
	var topics []string

	for index < len(data) {
		topicFilter, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topicFilter)
	}

	packet.Topics = topics

	return packet, nil
}

// parseUnsubAckV5 parses an UNSUBACK packet
func parseUnsubAckV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Properties
	properties, userProperties, err := decodeProperties(data, &index)
	if err != nil {
		return nil, err
	}
	packet.Properties = properties
	packet.UserProperties = userProperties

	// Reason Codes
	var reasonCodes []byte
	for index < len(data) {
		reasonCodes = append(reasonCodes, data[index])
		index++
	}

	packet.QoSs = reasonCodes // Reuse QoSs field for reason codes

	return packet, nil
}

// parseDisconnectV5 parses a DISCONNECT packet
func parseDisconnectV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	index := 0

	// Reason Code (if available)
	if index < len(data) {
		packet.ReasonCode = data[index]
		index++

		// Properties (if available)
		if index < len(data) {
			properties, userProperties, err := decodeProperties(data, &index)
			if err != nil {
				return nil, err
			}
			packet.Properties = properties
			packet.UserProperties = userProperties
		}
	} else {
		// If no reason code, assume normal disconnection (0)
		packet.ReasonCode = NORMAL_DISCONNECTION
	}

	return packet, nil
}

// parseAuthV5 parses an AUTH packet
func parseAuthV5(packet *PacketV5, data []byte, flags byte) (*PacketV5, error) {
	index := 0

	// Reason Code (if available)
	if index < len(data) {
		packet.ReasonCode = data[index]
		index++

		// Properties (if available)
		if index < len(data) {
			properties, userProperties, err := decodeProperties(data, &index)
			if err != nil {
				return nil, err
			}
			packet.Properties = properties
			packet.UserProperties = userProperties
		}
	} else {
		// If no reason code, assume 0
		packet.ReasonCode = 0
	}

	return packet, nil
}
