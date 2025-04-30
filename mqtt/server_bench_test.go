package mqtt

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// BenchmarkHandleConnection tests the performance of handling a new connection
func BenchmarkHandleConnection(b *testing.B) {
	// Create a server
	server := NewServer("localhost", 1883)

	// Create a connect packet
	connectPacket := NewConnectPacket("bench-client", "user", []byte("pass"), true)
	connectData, _ := connectPacket.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a mock connection with the connect packet data
		conn := &mockConn{
			readData: connectData,
		}

		// Process the connection
		go server.handleConnection(conn)

		// Give it a bit of time to process
		time.Sleep(1 * time.Millisecond)
	}
}

// BenchmarkPublishDistribution tests the performance of message distribution
func BenchmarkPublishDistribution(b *testing.B) {
	// Create a server
	server := NewServer("localhost", 1883)

	// Create client and register subscriptions directly
	clientID := "bench-client"
	conn := &mockConn{}

	// Add client to server
	client := NewClient(clientID, conn)
	server.clients[clientID] = client

	// Add subscription
	topic := "bench/topic"
	subscription := &Subscription{
		ClientID: clientID,
		Topic:    topic,
		QoS:      QoS1,
	}
	server.subscriptions[topic] = []*Subscription{subscription}

	// Prepare test payload
	payload := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Distribute message to subscribers
		server.distributeMessage(topic, payload, QoS1)
	}
}

// BenchmarkConcurrentConnections tests handling multiple concurrent connections
func BenchmarkConcurrentConnections(b *testing.B) {
	for _, concurrency := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(concurrency), func(b *testing.B) {
			// Create a server
			server := NewServer("localhost", 1883)

			// Create a connect packet
			connectPacket := NewConnectPacket("bench-client", "user", []byte("pass"), true)
			connectData, _ := connectPacket.Encode()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(concurrency)

				for j := 0; j < concurrency; j++ {
					go func(id int) {
						defer wg.Done()

						// Create a mock connection
						conn := &mockConn{
							readData: connectData,
						}

						// Add client directly to the server
						clientID := "bench-client-" + strconv.Itoa(id)
						client := NewClient(clientID, conn)

						server.clientsMutex.Lock()
						server.clients[clientID] = client
						server.clientsMutex.Unlock()

						// Process a packet
						server.handleConnect(conn, connectPacket)
					}(j)
				}

				wg.Wait()
			}
		})
	}
}

// BenchmarkTopicMatching tests the performance of topic subscription matching
func BenchmarkTopicMatching(b *testing.B) {
	// Set up subscriptions with different patterns
	subscriptions := []struct {
		name    string
		pattern string
		topic   string
	}{
		{"ExactMatch", "test/topic", "test/topic"},
		{"SingleWildcard", "test/+/data", "test/sensor/data"},
		{"MultiWildcard", "test/#", "test/sensor/data/temperature"},
		{"MixedWildcards", "test/+/data/#", "test/sensor/data/temperature/celsius"},
		{"NoMatch", "test/sensor", "other/topic"},
	}

	for _, sub := range subscriptions {
		b.Run(sub.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = topicMatches(sub.pattern, sub.topic)
			}
		})
	}
}

// BenchmarkRetainedMessages tests the performance of retained message handling
func BenchmarkRetainedMessages(b *testing.B) {
	// Create a server
	server := NewServer("localhost", 1883)

	// Set up some retained messages
	for i := 0; i < 100; i++ {
		topic := "retain/topic/" + strconv.Itoa(i)
		server.storeRetainedMessage(topic, []byte("retained data"), QoS1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a new client
		clientID := "bench-client-" + strconv.Itoa(i%1000)
		client := NewClient(clientID, &mockConn{})

		server.clientsMutex.Lock()
		server.clients[clientID] = client
		server.clientsMutex.Unlock()

		// Send retained messages to client
		server.sendRetainedMessages(client, []string{"retain/#"})
	}
}
