package mqtt

import (
	"testing"
)

func TestSharedSubscriptions(t *testing.T) {
	t.Run("Create shared subscription", func(t *testing.T) {
		// Create a shared subscription
		sub := NewSubscription("$share/group1/sensors/temperature", 1, "client1")

		if !sub.IsShared {
			t.Error("Expected subscription to be shared")
		}

		if sub.ShareGroup != "group1" {
			t.Errorf("Expected share group 'group1', got '%s'", sub.ShareGroup)
		}

		// Test topic matching with the shared subscription pattern
		if !sub.MatchesTopic("sensors/temperature") {
			t.Error("Expected to match 'sensors/temperature'")
		}
	})

	t.Run("Invalid shared subscription format", func(t *testing.T) {
		// Invalid format (missing group name)
		sub := NewSubscription("$share//sensors/temperature", 1, "client1")

		// Should still be recognized as shared even with invalid format
		if !sub.IsShared {
			t.Error("Expected subscription to be marked as shared")
		}

		// Empty share group
		if sub.ShareGroup != "" {
			t.Errorf("Expected empty share group, got '%s'", sub.ShareGroup)
		}

		// Invalid format (just $share without separator)
		sub = NewSubscription("$share", 1, "client1")

		// Should not be recognized as shared due to missing separator
		if sub.IsShared {
			t.Error("Expected subscription not to be marked as shared")
		}
	})

	t.Run("Multiple shares same group", func(t *testing.T) {
		// Create shared subscriptions with the same group
		sub1 := NewSubscription("$share/group1/sensors/temperature", 1, "client1")
		sub2 := NewSubscription("$share/group1/sensors/temperature", 1, "client2")
		sub3 := NewSubscription("$share/group1/sensors/temperature", 1, "client3")

		// All should be recognized as shared
		if !sub1.IsShared || !sub2.IsShared || !sub3.IsShared {
			t.Error("Expected all subscriptions to be marked as shared")
		}

		// All should have the same share group
		if sub1.ShareGroup != "group1" || sub2.ShareGroup != "group1" || sub3.ShareGroup != "group1" {
			t.Error("Expected all subscriptions to have share group 'group1'")
		}

		// All should match the same topic
		if !sub1.MatchesTopic("sensors/temperature") ||
			!sub2.MatchesTopic("sensors/temperature") ||
			!sub3.MatchesTopic("sensors/temperature") {
			t.Error("Expected all subscriptions to match 'sensors/temperature'")
		}
	})

	t.Run("Different share groups", func(t *testing.T) {
		// Create shared subscriptions with different groups
		sub1 := NewSubscription("$share/group1/sensors/temperature", 1, "client1")
		sub2 := NewSubscription("$share/group2/sensors/temperature", 1, "client2")

		// Both should be recognized as shared
		if !sub1.IsShared || !sub2.IsShared {
			t.Error("Expected both subscriptions to be marked as shared")
		}

		// Should have different share groups
		if sub1.ShareGroup == sub2.ShareGroup {
			t.Errorf("Expected different share groups, got '%s' for both", sub1.ShareGroup)
		}

		// Both should match the same topic
		if !sub1.MatchesTopic("sensors/temperature") || !sub2.MatchesTopic("sensors/temperature") {
			t.Error("Expected both subscriptions to match 'sensors/temperature'")
		}
	})

	t.Run("Shared subscription with wildcards", func(t *testing.T) {
		// Create shared subscription with wildcards
		sub := NewSubscription("$share/group1/sensors/+/temperature/#", 1, "client1")

		if !sub.IsShared {
			t.Error("Expected subscription to be shared")
		}

		if sub.ShareGroup != "group1" {
			t.Errorf("Expected share group 'group1', got '%s'", sub.ShareGroup)
		}

		// Test topic matching with wildcards
		if !sub.MatchesTopic("sensors/living-room/temperature") {
			t.Error("Expected to match 'sensors/living-room/temperature'")
		}

		if !sub.MatchesTopic("sensors/kitchen/temperature/celsius") {
			t.Error("Expected to match 'sensors/kitchen/temperature/celsius'")
		}

		if sub.MatchesTopic("sensors/kitchen/humidity") {
			t.Error("Expected not to match 'sensors/kitchen/humidity'")
		}
	})
}

func TestMatchSharedTopicPattern(t *testing.T) {
	testCases := []struct {
		name         string
		sharePattern string
		publishTopic string
		shouldMatch  bool
	}{
		{"Basic shared subscription", "$share/group1/topic", "topic", true},
		{"Shared with single-level wildcard", "$share/group1/topic/+", "topic/value", true},
		{"Shared with multi-level wildcard", "$share/group1/topic/#", "topic/a/b/c", true},
		{"No match on different topic", "$share/group1/topic", "other", false},
		{"Missing topic section", "$share/group1/", "topic", false},
		{"Invalid shared format", "$share/", "topic", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sub := NewSubscription(tc.sharePattern, 0, "client1")
			result := sub.MatchesTopic(tc.publishTopic)

			if result != tc.shouldMatch {
				t.Errorf("Expected match=%v for shared subscription pattern '%s' and publish topic '%s', got %v",
					tc.shouldMatch, tc.sharePattern, tc.publishTopic, result)
			}
		})
	}
}
