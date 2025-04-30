package mqtt

import (
	"bytes"
	"testing"
)

func TestMQTTv5PacketEncoding(t *testing.T) {
	t.Run("CONNECT with v5.0 properties", func(t *testing.T) {
		// Create a v5 connect packet
		packet := NewConnectPacketV5("client123", "username", []byte("password"), true)

		// Set v5 specific properties
		packet.AddProperty(SESSION_EXPIRY_INTERVAL, 3600)
		packet.AddProperty(RECEIVE_MAXIMUM, 100)
		packet.AddProperty(MAXIMUM_PACKET_SIZE, 1024)
		packet.AddProperty(TOPIC_ALIAS_MAXIMUM, 10)
		packet.AddProperty(REQUEST_RESPONSE_INFORMATION, byte(1))
		packet.AddProperty(REQUEST_PROBLEM_INFORMATION, byte(1))

		// Add user properties
		packet.AddUserProperty("app", "GoMQTT")
		packet.AddUserProperty("version", "1.0")

		// Encode it
		encoded, err := packet.EncodeV5()
		if err != nil {
			t.Fatalf("Failed to encode CONNECT packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacketV5(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != CONNECT {
			t.Errorf("Expected packet type %d, got %d", CONNECT, decoded.PacketType)
		}
		if decoded.ProtocolVersion != MQTT_5_0 {
			t.Errorf("Expected protocol version %d, got %d", MQTT_5_0, decoded.ProtocolVersion)
		}
		if decoded.ClientID != "client123" {
			t.Errorf("Expected ClientID 'client123', got '%s'", decoded.ClientID)
		}
		if decoded.Username != "username" {
			t.Errorf("Expected Username 'username', got '%s'", decoded.Username)
		}
		if string(decoded.Password) != "password" {
			t.Errorf("Expected Password 'password', got '%s'", string(decoded.Password))
		}
		if !decoded.CleanStart {
			t.Error("Expected CleanStart to be true")
		}

		// TODO: Check v5 properties
	})

	t.Run("PUBLISH with v5.0 properties", func(t *testing.T) {
		// Create a v5 publish packet
		packet := NewPublishPacketV5("test/topic", []byte("hello world"), 1, 123, false, true)

		// Set v5 specific properties
		packet.AddProperty(MESSAGE_EXPIRY_INTERVAL, 3600)
		packet.AddProperty(TOPIC_ALIAS, 5)
		packet.AddProperty(RESPONSE_TOPIC, "response/topic")
		packet.AddProperty(CORRELATION_DATA, []byte("correlation123"))
		packet.AddProperty(CONTENT_TYPE, "text/plain")

		// Add user properties
		packet.AddUserProperty("source", "sensor")

		// Encode it
		encoded, err := packet.EncodeV5()
		if err != nil {
			t.Fatalf("Failed to encode PUBLISH packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacketV5(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != PUBLISH {
			t.Errorf("Expected packet type %d, got %d", PUBLISH, decoded.PacketType)
		}
		if decoded.TopicName != "test/topic" {
			t.Errorf("Expected TopicName 'test/topic', got '%s'", decoded.TopicName)
		}
		if string(decoded.Payload) != "hello world" {
			t.Errorf("Expected Payload 'hello world', got '%s'", string(decoded.Payload))
		}

		// TODO: Check v5 properties
	})

	t.Run("SUBSCRIBE with v5.0 properties", func(t *testing.T) {
		// Create a v5 subscribe packet
		topics := []string{"topic1", "topic2"}
		qos := []byte{0, 1}
		packet := NewSubscribePacketV5(topics, qos, 456)

		// Set subscription identifier
		packet.AddProperty(SUBSCRIPTION_IDENTIFIER, 789)

		// Add user properties
		packet.AddUserProperty("type", "sensor")

		// Encode it
		encoded, err := packet.EncodeV5()
		if err != nil {
			t.Fatalf("Failed to encode SUBSCRIBE packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacketV5(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != SUBSCRIBE {
			t.Errorf("Expected packet type %d, got %d", SUBSCRIBE, decoded.PacketType)
		}
		if len(decoded.Topics) != 2 {
			t.Errorf("Expected 2 topics, got %d", len(decoded.Topics))
		}

		// TODO: Check subscription options
	})

	t.Run("DISCONNECT with v5.0 properties", func(t *testing.T) {
		// Create a v5 disconnect packet
		packet := NewDisconnectPacketV5(SERVER_SHUTTING_DOWN)

		// Set v5 specific properties
		packet.AddProperty(SESSION_EXPIRY_INTERVAL, 0)
		packet.AddProperty(REASON_STRING, "Server maintenance")

		// Add user properties
		packet.AddUserProperty("maintenance", "scheduled")

		// Encode it
		encoded, err := packet.EncodeV5()
		if err != nil {
			t.Fatalf("Failed to encode DISCONNECT packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacketV5(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != DISCONNECT {
			t.Errorf("Expected packet type %d, got %d", DISCONNECT, decoded.PacketType)
		}
		if decoded.ReasonCode != SERVER_SHUTTING_DOWN {
			t.Errorf("Expected ReasonCode %d, got %d", SERVER_SHUTTING_DOWN, decoded.ReasonCode)
		}

		// TODO: Check properties
	})
}

func TestSharedSubscription(t *testing.T) {
	t.Run("Parse shared subscription topic", func(t *testing.T) {
		// Create a subscription with shared topic format
		sub := NewSubscription("$share/group1/sensors/#", 1, "client123")

		if !sub.IsShared {
			t.Error("Expected IsShared to be true")
		}

		if sub.ShareGroup != "group1" {
			t.Errorf("Expected ShareGroup 'group1', got '%s'", sub.ShareGroup)
		}

		// Test topic matching
		if !sub.MatchesTopic("sensors/temperature") {
			t.Error("Expected to match 'sensors/temperature'")
		}

		if !sub.MatchesTopic("sensors/humidity/living-room") {
			t.Error("Expected to match 'sensors/humidity/living-room'")
		}

		if sub.MatchesTopic("other/sensors") {
			t.Error("Expected not to match 'other/sensors'")
		}
	})
}

func TestPropertyParsing(t *testing.T) {
	t.Run("Parse and encode property length", func(t *testing.T) {
		testCases := []struct {
			name   string
			length int
		}{
			{"Small property length", 127},
			{"Medium property length", 128},
			{"Large property length", 16383},
			{"Very large property length", 16384},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Encode property length
				buf := &bytes.Buffer{}
				encodeVarByteInt(buf, tc.length)
				encoded := buf.Bytes()

				// Decode property length
				var index int
				decoded, err := readVarByteInt(encoded, &index)
				if err != nil {
					t.Fatalf("Failed to decode property length: %v", err)
				}

				if decoded != tc.length {
					t.Errorf("Expected property length %d, got %d", tc.length, decoded)
				}
			})
		}
	})

	// Skip user property tests for now as they need implementation or adjustment
}
