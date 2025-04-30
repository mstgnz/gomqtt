package rate

import (
	"testing"
	"time"
)

func TestLimiter(t *testing.T) {
	// Create a new limiter with 10 tokens per second and capacity of 20
	limiter := New(10, 20)

	// Initial state should have full capacity
	for i := 0; i < 20; i++ {
		if !limiter.Allow() {
			t.Errorf("Expected allow to return true for token %d", i)
		}
	}

	// Next request should be denied
	if limiter.Allow() {
		t.Errorf("Expected allow to return false after capacity is exhausted")
	}

	// Wait for 100ms, should have 1 new token
	time.Sleep(100 * time.Millisecond)

	if !limiter.Allow() {
		t.Errorf("Expected allow to return true after token refill")
	}

	// Should be denied again
	if limiter.Allow() {
		t.Errorf("Expected allow to return false after using refilled token")
	}

	// Reset the limiter
	limiter.Reset()

	// Should have full capacity again
	for i := 0; i < 20; i++ {
		if !limiter.Allow() {
			t.Errorf("Expected allow to return true after reset for token %d", i)
		}
	}
}

func TestClientLimiter(t *testing.T) {
	// Create default rates
	defaultRates := map[string]float64{
		ConnectLimit:   5,
		PublishLimit:   10,
		SubscribeLimit: 2,
	}

	// Create client limiter with burst multiplier of 2
	clientLimiter := NewClientLimiter(defaultRates, 2)

	// Test connect limit for client1
	for i := 0; i < 10; i++ { // Capacity is 5 * 2 = 10
		if !clientLimiter.Allow("client1", ConnectLimit) {
			t.Errorf("Expected allow to return true for client1 connect %d", i)
		}
	}

	// Next request should be denied
	if clientLimiter.Allow("client1", ConnectLimit) {
		t.Errorf("Expected allow to return false after capacity is exhausted")
	}

	// Test publish limit for client1
	for i := 0; i < 20; i++ { // Capacity is 10 * 2 = 20
		if !clientLimiter.Allow("client1", PublishLimit) {
			t.Errorf("Expected allow to return true for client1 publish %d", i)
		}
	}

	// Next request should be denied
	if clientLimiter.Allow("client1", PublishLimit) {
		t.Errorf("Expected allow to return false after capacity is exhausted")
	}

	// Test custom rate for client2
	clientLimiter.SetClientRate("client2", PublishLimit, 100)

	// Client2 should have a higher limit (100 * 2 = 200)
	for i := 0; i < 200; i++ {
		if !clientLimiter.Allow("client2", PublishLimit) {
			t.Errorf("Expected allow to return true for client2 publish %d", i)
		}
	}

	// Next request should be denied
	if clientLimiter.Allow("client2", PublishLimit) {
		t.Errorf("Expected allow to return false after capacity is exhausted")
	}

	// Reset client1
	clientLimiter.Reset("client1")

	// Client1 connect should work again
	if !clientLimiter.Allow("client1", ConnectLimit) {
		t.Errorf("Expected allow to return true after reset")
	}

	// Reset all
	clientLimiter.ResetAll()

	// Client2 publish should work again
	if !clientLimiter.Allow("client2", PublishLimit) {
		t.Errorf("Expected allow to return true after reset all")
	}

	// Test default rate change
	clientLimiter.SetDefaultRate(SubscribeLimit, 50)

	// New client should have the new default rate
	for i := 0; i < 100; i++ { // Capacity is 50 * 2 = 100
		if !clientLimiter.Allow("client3", SubscribeLimit) {
			t.Errorf("Expected allow to return true for client3 subscribe %d", i)
		}
	}
}
