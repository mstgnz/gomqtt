package mqtt

import (
	"bytes"
	"io"
	"testing"
)

// BenchmarkEncodeConnectPacket tests the performance of encoding a CONNECT packet
func BenchmarkEncodeConnectPacket(b *testing.B) {
	packet := NewConnectPacket("client-id", "username", []byte("password"), true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = packet.Encode()
	}
}

// BenchmarkEncodePublishPacket tests the performance of encoding a PUBLISH packet
func BenchmarkEncodePublishPacket(b *testing.B) {
	payload := []byte("Hello, MQTT!")
	packet := NewPublishPacket("test/topic", payload, QoS1, 1234, false, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = packet.Encode()
	}
}

// BenchmarkEncodeSubscribePacket tests the performance of encoding a SUBSCRIBE packet
func BenchmarkEncodeSubscribePacket(b *testing.B) {
	topics := []string{"topic/1", "topic/2", "topic/3"}
	qos := []byte{QoS0, QoS1, QoS2}
	packet := NewSubscribePacket(topics, qos, 5678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = packet.Encode()
	}
}

// BenchmarkDecodeConnectPacket tests the performance of decoding a CONNECT packet
func BenchmarkDecodeConnectPacket(b *testing.B) {
	packet := NewConnectPacket("client-id", "username", []byte("password"), true)
	data, _ := packet.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)
		reader.Seek(0, io.SeekStart)
	}
}

// BenchmarkDecodePublishPacket tests the performance of decoding a PUBLISH packet
func BenchmarkDecodePublishPacket(b *testing.B) {
	payload := []byte("Hello, MQTT!")
	packet := NewPublishPacket("test/topic", payload, QoS1, 1234, false, false)
	data, _ := packet.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)
		reader.Seek(0, io.SeekStart)
	}
}

// BenchmarkDecodeSubscribePacket tests the performance of decoding a SUBSCRIBE packet
func BenchmarkDecodeSubscribePacket(b *testing.B) {
	topics := []string{"topic/1", "topic/2", "topic/3"}
	qos := []byte{QoS0, QoS1, QoS2}
	packet := NewSubscribePacket(topics, qos, 5678)
	data, _ := packet.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)
		reader.Seek(0, io.SeekStart)
	}
}

// BenchmarkQoS0PublishFlow simulates a complete QoS0 publish flow
func BenchmarkQoS0PublishFlow(b *testing.B) {
	payload := []byte("Hello, MQTT!")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create and encode publish packet
		publishPacket := NewPublishPacket("test/topic", payload, QoS0, 0, false, false)
		data, _ := publishPacket.Encode()

		// Decode the packet (simulating receiving it)
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)
	}
}

// BenchmarkQoS1PublishFlow simulates a complete QoS1 publish flow
func BenchmarkQoS1PublishFlow(b *testing.B) {
	payload := []byte("Hello, MQTT!")
	messageID := uint16(1234)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create and encode publish packet
		publishPacket := NewPublishPacket("test/topic", payload, QoS1, messageID, false, false)
		data, _ := publishPacket.Encode()

		// Decode the packet (simulating receiving it)
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)

		// Create and encode PUBACK packet
		pubAckPacket := NewPubAckPacket(messageID)
		_, _ = pubAckPacket.Encode()
	}
}

// BenchmarkQoS2PublishFlow simulates a complete QoS2 publish flow
func BenchmarkQoS2PublishFlow(b *testing.B) {
	payload := []byte("Hello, MQTT!")
	messageID := uint16(5678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create and encode publish packet
		publishPacket := NewPublishPacket("test/topic", payload, QoS2, messageID, false, false)
		data, _ := publishPacket.Encode()

		// Decode the packet (simulating receiving it)
		reader := bytes.NewReader(data)
		_, _ = ReadPacket(reader)

		// PUBREC
		pubRecPacket := NewPubRecPacket(messageID)
		_, _ = pubRecPacket.Encode()

		// PUBREL
		pubRelPacket := NewPubRelPacket(messageID)
		_, _ = pubRelPacket.Encode()

		// PUBCOMP
		pubCompPacket := NewPubCompPacket(messageID)
		_, _ = pubCompPacket.Encode()
	}
}

// BenchmarkEncodeRemainingLength tests encoding of the remaining length field
func BenchmarkEncodeRemainingLength(b *testing.B) {
	lengths := []int{20, 200, 2000, 20000, 200000}

	for _, length := range lengths {
		b.Run(string(byte(length)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = encodeRemainingLength(length)
			}
		})
	}
}

// BenchmarkReadRemainingLength tests reading of the remaining length field
func BenchmarkReadRemainingLength(b *testing.B) {
	// Generate encoded remaining lengths of different sizes
	testCases := []struct {
		name   string
		length int
	}{
		{"Small", 20},
		{"Medium", 2000},
		{"Large", 200000},
	}

	for _, tc := range testCases {
		encoded := encodeRemainingLength(tc.length)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(encoded)
				_, _ = readRemainingLength(reader)
			}
		})
	}
}
