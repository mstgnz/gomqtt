package mqtt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MQTT packet types as defined in the MQTT 3.1.1 spec
const (
	RESERVED    = 0
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

// Connect return codes
const (
	ConnAccepted                     = 0x00
	ConnRefusedUnacceptableProtocol  = 0x01
	ConnRefusedIdentifierRejected    = 0x02
	ConnRefusedServerUnavailable     = 0x03
	ConnRefusedBadUsernameOrPassword = 0x04
	ConnRefusedNotAuthorized         = 0x05
)

// ErrInvalidPacket is returned when packet parsing fails
var ErrInvalidPacket = errors.New("invalid MQTT packet")

// Packet represents an MQTT control packet
type Packet struct {
	// Fixed header
	PacketType byte
	Dup        bool
	Qos        byte
	Retain     bool

	// Variable header fields - common across multiple packet types
	ProtocolName    string
	ProtocolVersion byte
	ConnectFlags    byte
	KeepAlive       uint16
	MessageID       uint16
	ReturnCode      byte

	// Payload fields
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
	Payload   []byte

	// For SUBSCRIBE/UNSUBSCRIBE
	Topics []string
	QoSs   []byte

	// Raw packet for easy retrieval
	rawBytes []byte
}

// NewConnectPacket creates a new CONNECT packet
func NewConnectPacket(clientID string, username string, password []byte, cleanSession bool) *Packet {
	packet := &Packet{
		PacketType:      CONNECT,
		ProtocolName:    "MQTT",
		ProtocolVersion: 4, // MQTT 3.1.1
		ClientID:        clientID,
		CleanSession:    cleanSession,
		KeepAlive:       60, // 60 seconds default
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

// NewWillConnectPacket creates a connect packet with a will message
func NewWillConnectPacket(clientID string, willTopic string, willMessage []byte, willQoS byte, willRetain bool) *Packet {
	packet := NewConnectPacket(clientID, "", nil, true)
	packet.WillTopic = willTopic
	packet.WillMessage = willMessage
	packet.WillQoS = willQoS
	packet.WillRetain = willRetain
	return packet
}

// NewPublishPacket creates a new PUBLISH packet
func NewPublishPacket(topic string, payload []byte, qos byte, messageID uint16, dup bool, retain bool) *Packet {
	return &Packet{
		PacketType: PUBLISH,
		TopicName:  topic,
		Payload:    payload,
		Qos:        qos,
		MessageID:  messageID,
		Dup:        dup,
		Retain:     retain,
	}
}

// NewPubAckPacket creates a new PUBACK packet
func NewPubAckPacket(messageID uint16) *Packet {
	return &Packet{
		PacketType: PUBACK,
		MessageID:  messageID,
	}
}

// NewPubRecPacket creates a new PUBREC packet
func NewPubRecPacket(messageID uint16) *Packet {
	return &Packet{
		PacketType: PUBREC,
		MessageID:  messageID,
	}
}

// NewPubRelPacket creates a new PUBREL packet
func NewPubRelPacket(messageID uint16) *Packet {
	return &Packet{
		PacketType: PUBREL,
		MessageID:  messageID,
	}
}

// NewPubCompPacket creates a new PUBCOMP packet
func NewPubCompPacket(messageID uint16) *Packet {
	return &Packet{
		PacketType: PUBCOMP,
		MessageID:  messageID,
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

// NewSubAckPacket creates a new SUBACK packet
func NewSubAckPacket(messageID uint16, returnCodes []byte) *Packet {
	return &Packet{
		PacketType: SUBACK,
		MessageID:  messageID,
		QoSs:       returnCodes, // Using QoSs field to store return codes
	}
}

// NewUnsubscribePacket creates a new UNSUBSCRIBE packet
func NewUnsubscribePacket(topics []string, messageID uint16) *Packet {
	return &Packet{
		PacketType: UNSUBSCRIBE,
		Topics:     topics,
		MessageID:  messageID,
	}
}

// NewUnsubAckPacket creates a new UNSUBACK packet
func NewUnsubAckPacket(messageID uint16) *Packet {
	return &Packet{
		PacketType: UNSUBACK,
		MessageID:  messageID,
	}
}

// NewPingReqPacket creates a new PINGREQ packet
func NewPingReqPacket() *Packet {
	return &Packet{
		PacketType: PINGREQ,
	}
}

// NewPingRespPacket creates a new PINGRESP packet
func NewPingRespPacket() *Packet {
	return &Packet{
		PacketType: PINGRESP,
	}
}

// NewDisconnectPacket creates a new DISCONNECT packet
func NewDisconnectPacket() *Packet {
	return &Packet{
		PacketType: DISCONNECT,
	}
}

// NewConnAckPacket creates a new CONNACK packet
func NewConnAckPacket(sessionPresent bool, returnCode byte) *Packet {
	packet := &Packet{
		PacketType: CONNACK,
		ReturnCode: returnCode,
	}

	if sessionPresent {
		packet.ConnectFlags = 1 // Bit 0 is the SP (Session Present) flag
	}

	return packet
}

// Encode serializes the packet into bytes ready for transmission
func (p *Packet) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write temporary header (will be replaced later when we know the remaining length)
	buf.WriteByte(0) // Placeholder

	// Variable header + payload buffer
	var variableAndPayload bytes.Buffer
	var err error

	// Encode the variable header and payload for each packet type
	switch p.PacketType {
	case CONNECT:
		err = p.encodeConnect(&variableAndPayload)
	case CONNACK:
		err = p.encodeConnAck(&variableAndPayload)
	case PUBLISH:
		err = p.encodePublish(&variableAndPayload)
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:
		err = p.encodeMessageIDOnly(&variableAndPayload)
	case SUBSCRIBE:
		err = p.encodeSubscribe(&variableAndPayload)
	case SUBACK:
		err = p.encodeSubAck(&variableAndPayload)
	case UNSUBSCRIBE:
		err = p.encodeUnsubscribe(&variableAndPayload)
	case PINGREQ, PINGRESP, DISCONNECT:
		// These packets have no variable header or payload
		err = nil
	default:
		return nil, fmt.Errorf("unsupported packet type: %d", p.PacketType)
	}

	if err != nil {
		return nil, err
	}

	// Encode the remaining length
	remainingLength := variableAndPayload.Len()
	remainingLengthBytes := encodeRemainingLength(remainingLength)

	// Now build the final packet
	result := make([]byte, 1+len(remainingLengthBytes)+remainingLength)

	// Fixed header - first byte
	firstByte := p.PacketType << 4

	// Add flags if applicable
	if p.PacketType == PUBLISH {
		if p.Dup {
			firstByte |= 0x08
		}
		firstByte |= (p.Qos << 1) & 0x06
		if p.Retain {
			firstByte |= 0x01
		}
	} else if p.PacketType == PUBREL || p.PacketType == SUBSCRIBE || p.PacketType == UNSUBSCRIBE {
		// These packets have the reserved flag bits set to 0,0,1,0
		firstByte |= 0x02
	}

	result[0] = byte(firstByte)

	// Add remaining length
	copy(result[1:], remainingLengthBytes)

	// Add variable header and payload
	copy(result[1+len(remainingLengthBytes):], variableAndPayload.Bytes())

	return result, nil
}

// encodeString writes a UTF-8 string to the buffer in MQTT format (2-byte length + data)
func encodeString(buf *bytes.Buffer, str string) error {
	strLen := len(str)
	if strLen > 65535 {
		return fmt.Errorf("string too long: %d bytes", strLen)
	}

	binary.Write(buf, binary.BigEndian, uint16(strLen))
	buf.WriteString(str)
	return nil
}

// encodeBytes writes a byte array to the buffer in MQTT format (2-byte length + data)
func encodeBytes(buf *bytes.Buffer, data []byte) error {
	dataLen := len(data)
	if dataLen > 65535 {
		return fmt.Errorf("data too long: %d bytes", dataLen)
	}

	binary.Write(buf, binary.BigEndian, uint16(dataLen))
	buf.Write(data)
	return nil
}

// encodeConnect encodes the variable header and payload for CONNECT packets
func (p *Packet) encodeConnect(buf *bytes.Buffer) error {
	// Protocol name
	encodeString(buf, p.ProtocolName)

	// Protocol version
	buf.WriteByte(p.ProtocolVersion)

	// Connect flags
	var connectFlags byte
	if p.CleanSession {
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

	// Payload - Client ID
	encodeString(buf, p.ClientID)

	// Will topic & message
	if p.WillTopic != "" {
		encodeString(buf, p.WillTopic)
		encodeBytes(buf, p.WillMessage)
	}

	// Username & password
	if p.Username != "" {
		encodeString(buf, p.Username)
		if len(p.Password) > 0 {
			encodeBytes(buf, p.Password)
		}
	}

	return nil
}

// encodeConnAck encodes the variable header for CONNACK packets
func (p *Packet) encodeConnAck(buf *bytes.Buffer) error {
	// Acknowledge flags
	buf.WriteByte(p.ConnectFlags & 0x01) // Session present flag

	// Return code
	buf.WriteByte(p.ReturnCode)

	return nil
}

// encodePublish encodes the variable header and payload for PUBLISH packets
func (p *Packet) encodePublish(buf *bytes.Buffer) error {
	// Topic name
	encodeString(buf, p.TopicName)

	// Message ID - only included for QoS > 0
	if p.Qos > 0 {
		binary.Write(buf, binary.BigEndian, p.MessageID)
	}

	// Payload
	buf.Write(p.Payload)

	return nil
}

// encodeMessageIDOnly encodes packets that only have a message ID in their variable header
func (p *Packet) encodeMessageIDOnly(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, p.MessageID)
}

// encodeSubscribe encodes the variable header and payload for SUBSCRIBE packets
func (p *Packet) encodeSubscribe(buf *bytes.Buffer) error {
	// Message ID
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Payload: topic filters and QoS
	for i, topic := range p.Topics {
		encodeString(buf, topic)
		if i < len(p.QoSs) {
			buf.WriteByte(p.QoSs[i] & 0x03) // QoS value, masked to 2 bits
		} else {
			buf.WriteByte(0) // Default to QoS 0 if not specified
		}
	}

	return nil
}

// encodeSubAck encodes the variable header and payload for SUBACK packets
func (p *Packet) encodeSubAck(buf *bytes.Buffer) error {
	// Message ID
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Payload: return codes
	buf.Write(p.QoSs) // Using QoSs field to store return codes

	return nil
}

// encodeUnsubscribe encodes the variable header and payload for UNSUBSCRIBE packets
func (p *Packet) encodeUnsubscribe(buf *bytes.Buffer) error {
	// Message ID
	binary.Write(buf, binary.BigEndian, p.MessageID)

	// Payload: topic filters
	for _, topic := range p.Topics {
		encodeString(buf, topic)
	}

	return nil
}

// encodeRemainingLength encodes the remaining length according to the MQTT variable length spec
func encodeRemainingLength(length int) []byte {
	var result []byte

	for {
		digit := byte(length % 128)
		length = length / 128

		if length > 0 {
			digit = digit | 0x80
		}

		result = append(result, digit)

		if length == 0 {
			break
		}
	}

	return result
}

// ReadPacket reads and parses an MQTT packet from a reader
func ReadPacket(reader io.Reader) (*Packet, error) {
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
	packet := &Packet{
		PacketType: packetType,
		rawBytes:   append([]byte{header[0]}, buf...),
	}

	// Parse according to packet type
	switch packetType {
	case CONNECT:
		return parseConnect(packet, buf, flags)
	case CONNACK:
		return parseConnAck(packet, buf, flags)
	case PUBLISH:
		packet.Dup = (flags & 0x08) > 0
		packet.Qos = (flags & 0x06) >> 1
		packet.Retain = (flags & 0x01) > 0
		return parsePublish(packet, buf)
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:
		return parseMessageIDOnly(packet, buf, flags)
	case SUBSCRIBE:
		return parseSubscribe(packet, buf, flags)
	case SUBACK:
		return parseSubAck(packet, buf, flags)
	case UNSUBSCRIBE:
		return parseUnsubscribe(packet, buf, flags)
	case PINGREQ, PINGRESP, DISCONNECT:
		// These packets have no variable header or payload
		if len(buf) > 0 {
			return nil, ErrInvalidPacket
		}
		return packet, nil
	default:
		return nil, fmt.Errorf("unknown packet type: %d", packetType)
	}
}

// readRemainingLength reads the remaining length field from the reader
func readRemainingLength(reader io.Reader) (int, error) {
	var result int
	var multiplier int = 1
	var encodedByte byte

	for i := 0; i < 4; i++ {
		// Read the next byte
		if err := binary.Read(reader, binary.BigEndian, &encodedByte); err != nil {
			return 0, err
		}

		// Add to the result
		result += int(encodedByte&0x7F) * multiplier
		multiplier *= 128

		// Check if we're done
		if (encodedByte & 0x80) == 0 {
			break
		}

		// If we've read 4 bytes and the last one has continuation bit set, it's invalid
		if i == 3 && (encodedByte&0x80) != 0 {
			return 0, ErrInvalidPacket
		}
	}

	return result, nil
}

// readString reads a UTF-8 string in MQTT format (2-byte length + data)
func readString(data []byte, index *int) (string, error) {
	if len(data) < *index+2 {
		return "", ErrInvalidPacket
	}

	length := int(binary.BigEndian.Uint16(data[*index : *index+2]))
	*index += 2

	if len(data) < *index+length {
		return "", ErrInvalidPacket
	}

	str := string(data[*index : *index+length])
	*index += length

	return str, nil
}

// readBytes reads a byte array in MQTT format (2-byte length + data)
func readBytes(data []byte, index *int) ([]byte, error) {
	if len(data) < *index+2 {
		return nil, ErrInvalidPacket
	}

	length := int(binary.BigEndian.Uint16(data[*index : *index+2]))
	*index += 2

	if len(data) < *index+length {
		return nil, ErrInvalidPacket
	}

	bytes := data[*index : *index+length]
	*index += length

	return bytes, nil
}

// parseConnect parses a CONNECT packet
func parseConnect(packet *Packet, data []byte, flags byte) (*Packet, error) {
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
	packet.CleanSession = (connectFlags & 0x02) > 0
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

	// Client ID
	clientID, err := readString(data, &index)
	if err != nil {
		return nil, err
	}
	packet.ClientID = clientID

	// Will properties
	if willFlag {
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

// parseConnAck parses a CONNACK packet
func parseConnAck(packet *Packet, data []byte, flags byte) (*Packet, error) {
	if len(data) != 2 {
		return nil, ErrInvalidPacket
	}

	packet.ConnectFlags = data[0] // Session present flag
	packet.ReturnCode = data[1]

	return packet, nil
}

// parsePublish parses a PUBLISH packet
func parsePublish(packet *Packet, data []byte) (*Packet, error) {
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

	// Payload
	if index < len(data) {
		packet.Payload = data[index:]
	} else {
		packet.Payload = []byte{}
	}

	return packet, nil
}

// parseMessageIDOnly parses packets that only have a message ID in their variable header
func parseMessageIDOnly(packet *Packet, data []byte, flags byte) (*Packet, error) {
	if len(data) != 2 {
		return nil, ErrInvalidPacket
	}

	packet.MessageID = binary.BigEndian.Uint16(data)

	return packet, nil
}

// parseSubscribe parses a SUBSCRIBE packet
func parseSubscribe(packet *Packet, data []byte, flags byte) (*Packet, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Topic filters and QoS
	var topics []string
	var qos []byte

	for index < len(data) {
		// Topic filter
		topicFilter, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topicFilter)

		// QoS
		if index >= len(data) {
			return nil, ErrInvalidPacket
		}
		qos = append(qos, data[index]&0x03) // QoS value, masked to 2 bits
		index++
	}

	packet.Topics = topics
	packet.QoSs = qos

	return packet, nil
}

// parseSubAck parses a SUBACK packet
func parseSubAck(packet *Packet, data []byte, flags byte) (*Packet, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	packet.MessageID = binary.BigEndian.Uint16(data[0:2])

	// Return codes
	packet.QoSs = data[2:] // Using QoSs field to store return codes

	return packet, nil
}

// parseUnsubscribe parses an UNSUBSCRIBE packet
func parseUnsubscribe(packet *Packet, data []byte, flags byte) (*Packet, error) {
	if len(data) < 2 { // At least message ID
		return nil, ErrInvalidPacket
	}

	index := 0

	// Message ID
	packet.MessageID = binary.BigEndian.Uint16(data[index : index+2])
	index += 2

	// Topic filters
	var topics []string

	for index < len(data) {
		// Topic filter
		topicFilter, err := readString(data, &index)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topicFilter)
	}

	packet.Topics = topics

	return packet, nil
}
