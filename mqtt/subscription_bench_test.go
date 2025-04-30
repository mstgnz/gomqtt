package mqtt

import (
	"testing"
)

// BenchmarkSubscriptionMatching tests the performance of subscription topic matching
func BenchmarkSubscriptionMatching(b *testing.B) {
	// Define test cases with different patterns and topics
	tests := []struct {
		name    string
		pattern string
		topic   string
	}{
		{"ExactMatch", "test/topic", "test/topic"},
		{"SingleWildcard", "test/+/data", "test/sensor/data"},
		{"MultiWildcard", "test/#", "test/sensor/data/temperature"},
		{"MixedWildcards", "test/+/data/#", "test/sensor/data/temperature/celsius"},
		{"NoMatch", "test/sensor", "other/topic"},
		{"DeepTopic", "a/b/c/d/e/f/g/h", "a/b/c/d/e/f/g/h"},
		{"DeepWildcard", "a/+/+/+/+/+/+/+", "a/b/c/d/e/f/g/h"},
		{"ComplexWildcard", "a/+/b/+/c/+/d/#", "a/1/b/2/c/3/d/4/5/6"},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = topicMatches(test.pattern, test.topic)
			}
		})
	}
}

// BenchmarkSharedSubscriptionMatching tests the performance of $share subscription matching
func BenchmarkSharedSubscriptionMatching(b *testing.B) {
	// Test data for shared subscriptions
	tests := []struct {
		name    string
		pattern string // Shared subscription pattern
		topic   string // Published topic
	}{
		{"Simple", "$share/group1/topic", "topic"},
		{"WithWildcard", "$share/group1/topic/#", "topic/subtopic"},
		{"WithMultiWildcard", "$share/group1/a/+/c/#", "a/b/c/d/e"},
		{"ComplexGroup", "$share/complex-group-123/topic", "topic"},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Parse the shared subscription
				_, _, subTopic, isSharedSubscription := parseSharedSubscription(test.pattern)

				// Only check topic match if it's a shared subscription
				if isSharedSubscription {
					// Check if the published topic matches the subscription pattern
					_ = topicMatches(subTopic, test.topic)
				}
			}
		})
	}
}

// parseSharedSubscription parses a shared subscription topic
// Returns the original topic, group name, real topic pattern, and whether it's a shared subscription
func parseSharedSubscription(topic string) (string, string, string, bool) {
	// Check if it starts with $share/
	if len(topic) < 8 || topic[:7] != "$share/" {
		return topic, "", topic, false
	}

	// Find the second slash that separates the group name from the actual topic pattern
	idx := -1
	for i := 7; i < len(topic); i++ {
		if topic[i] == '/' {
			idx = i
			break
		}
	}

	// Invalid format if no second slash found
	if idx == -1 {
		return topic, "", topic, false
	}

	groupName := topic[7:idx]
	realTopic := topic[idx+1:]

	return topic, groupName, realTopic, true
}
