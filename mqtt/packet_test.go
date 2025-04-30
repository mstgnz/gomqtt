package mqtt

import (
	"bytes"
	"io"
	"testing"
)

func TestPacketEncoding(t *testing.T) {
	t.Run("Encode CONNECT packet", func(t *testing.T) {
		// Create a new CONNECT packet
		packet := NewConnectPacket("client123", "username", []byte("password"), true)

		// Encode it
		encoded, err := packet.Encode()
		if err != nil {
			t.Fatalf("Failed to encode CONNECT packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacket(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != CONNECT {
			t.Errorf("Expected packet type %d, got %d", CONNECT, decoded.PacketType)
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
		if !decoded.CleanSession {
			t.Error("Expected CleanSession to be true")
		}
	})

	t.Run("Encode PUBLISH packet", func(t *testing.T) {
		// Create a new PUBLISH packet
		packet := NewPublishPacket("test/topic", []byte("hello world"), 1, 123, false, true)

		// Encode it
		encoded, err := packet.Encode()
		if err != nil {
			t.Fatalf("Failed to encode PUBLISH packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacket(reader)
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
		if decoded.Qos != 1 {
			t.Errorf("Expected QoS 1, got %d", decoded.Qos)
		}
		if decoded.MessageID != 123 {
			t.Errorf("Expected MessageID 123, got %d", decoded.MessageID)
		}
		if decoded.Dup {
			t.Error("Expected Dup to be false")
		}
		if !decoded.Retain {
			t.Error("Expected Retain to be true")
		}
	})

	t.Run("Encode SUBSCRIBE packet", func(t *testing.T) {
		// Create a new SUBSCRIBE packet
		topics := []string{"topic1", "topic2"}
		qos := []byte{0, 1}
		packet := NewSubscribePacket(topics, qos, 456)

		// Encode it
		encoded, err := packet.Encode()
		if err != nil {
			t.Fatalf("Failed to encode SUBSCRIBE packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacket(reader)
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
		if decoded.Topics[0] != "topic1" {
			t.Errorf("Expected topic 'topic1', got '%s'", decoded.Topics[0])
		}
		if decoded.Topics[1] != "topic2" {
			t.Errorf("Expected topic 'topic2', got '%s'", decoded.Topics[1])
		}
		if decoded.QoSs[0] != 0 {
			t.Errorf("Expected QoS 0, got %d", decoded.QoSs[0])
		}
		if decoded.QoSs[1] != 1 {
			t.Errorf("Expected QoS 1, got %d", decoded.QoSs[1])
		}
		if decoded.MessageID != 456 {
			t.Errorf("Expected MessageID 456, got %d", decoded.MessageID)
		}
	})

	t.Run("Encode PINGREQ packet", func(t *testing.T) {
		// Create a new PINGREQ packet
		packet := NewPingReqPacket()

		// Encode it
		encoded, err := packet.Encode()
		if err != nil {
			t.Fatalf("Failed to encode PINGREQ packet: %v", err)
		}

		// Decode the encoded packet
		reader := bytes.NewReader(encoded)
		decoded, err := ReadPacket(reader)
		if err != nil {
			t.Fatalf("Failed to decode packet: %v", err)
		}

		// Validate fields
		if decoded.PacketType != PINGREQ {
			t.Errorf("Expected packet type %d, got %d", PINGREQ, decoded.PacketType)
		}
	})
}

func TestRemainingLength(t *testing.T) {
	testCases := []struct {
		name   string
		length int
		bytes  int
	}{
		{"Small length", 127, 1},
		{"Medium length", 128, 2},
		{"Large length", 16383, 2},
		{"Very large length", 16384, 3},
		{"Maximum length", 268435455, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeRemainingLength(tc.length)
			if len(encoded) != tc.bytes {
				t.Errorf("Expected %d bytes for length %d, got %d", tc.bytes, tc.length, len(encoded))
			}

			// Test decoding too
			reader := bytes.NewReader(encoded)
			decoded, err := readRemainingLength(reader)
			if err != nil {
				t.Fatalf("Failed to decode remaining length: %v", err)
			}
			if decoded != tc.length {
				t.Errorf("Expected decoded length %d, got %d", tc.length, decoded)
			}
		})
	}

	// Test error handling
	t.Run("Invalid remaining length", func(t *testing.T) {
		// Create an invalid remaining length encoding (5 continuation bytes)
		invalid := []byte{0x80, 0x80, 0x80, 0x80, 0x80}
		reader := bytes.NewReader(invalid)
		_, err := readRemainingLength(reader)
		if err == nil {
			t.Fatal("Expected error for invalid remaining length, got nil")
		}
	})
}

func TestStringEncoding(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"Empty string", ""},
		{"ASCII string", "hello world"},
		{"UTF-8 string", "こんにちは世界"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode string to buffer
			buf := &bytes.Buffer{}
			err := encodeString(buf, tc.input)
			if err != nil {
				t.Fatalf("Failed to encode string: %v", err)
			}

			// Decode string
			data := buf.Bytes()
			var index int
			decoded, err := readString(data, &index)
			if err != nil {
				t.Fatalf("Failed to decode string: %v", err)
			}

			if decoded != tc.input {
				t.Errorf("Expected string '%s', got '%s'", tc.input, decoded)
			}

			if index != len(data) {
				t.Errorf("Index not at end of buffer: %d != %d", index, len(data))
			}
		})
	}
}

func TestReadPacketError(t *testing.T) {
	t.Run("Empty reader", func(t *testing.T) {
		reader := bytes.NewReader([]byte{})
		_, err := ReadPacket(reader)
		if err != io.EOF {
			t.Errorf("Expected EOF error, got %v", err)
		}
	})

	t.Run("Truncated packet", func(t *testing.T) {
		// First byte of a CONNECT packet but nothing else
		reader := bytes.NewReader([]byte{0x10})
		_, err := ReadPacket(reader)
		if err != io.EOF {
			t.Errorf("Expected EOF error for truncated packet, got %v", err)
		}
	})

	t.Run("Invalid packet type", func(t *testing.T) {
		// 0xF0 is not a valid packet type (packet type 15 with reserved flags 0)
		// 0xF0 would be interpreted as packet type 15 (AUTH in MQTT 5.0) but we need to check against default error
		reader := bytes.NewReader([]byte{0xF0, 0x00})
		_, err := ReadPacket(reader)
		if err == nil || err.Error() != "unknown packet type: 15" {
			t.Errorf("Expected error 'unknown packet type: 15', got %v", err)
		}
	})
}
