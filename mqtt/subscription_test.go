package mqtt

import (
	"testing"
)

func TestSubscription(t *testing.T) {
	t.Run("Create subscription", func(t *testing.T) {
		clientID := "client123"
		topic := "sensors/temperature"
		qos := byte(1)

		sub := NewSubscription(topic, qos, clientID)

		if sub.Topic != topic {
			t.Errorf("Expected topic %s, got %s", topic, sub.Topic)
		}
		if sub.QoS != qos {
			t.Errorf("Expected QoS %d, got %d", qos, sub.QoS)
		}
		if sub.ClientID != clientID {
			t.Errorf("Expected ClientID %s, got %s", clientID, sub.ClientID)
		}
		if sub.IsShared {
			t.Error("Expected IsShared to be false")
		}
		if sub.ShareGroup != "" {
			t.Errorf("Expected empty ShareGroup, got %s", sub.ShareGroup)
		}
	})

	t.Run("Create shared subscription", func(t *testing.T) {
		clientID := "client123"
		topic := "$share/group1/sensors/temperature"
		qos := byte(1)

		sub := NewSubscription(topic, qos, clientID)

		if sub.Topic != topic {
			t.Errorf("Expected topic %s, got %s", topic, sub.Topic)
		}
		if sub.QoS != qos {
			t.Errorf("Expected QoS %d, got %d", qos, sub.QoS)
		}
		if sub.ClientID != clientID {
			t.Errorf("Expected ClientID %s, got %s", clientID, sub.ClientID)
		}
		if !sub.IsShared {
			t.Error("Expected IsShared to be true")
		}
		if sub.ShareGroup != "group1" {
			t.Errorf("Expected ShareGroup 'group1', got %s", sub.ShareGroup)
		}
	})
}

func TestTopicMatching(t *testing.T) {
	testCases := []struct {
		name             string
		subscribePattern string
		publishTopic     string
		shouldMatch      bool
	}{
		{"Exact match", "topic", "topic", true},
		{"Simple wildcard", "topic/+", "topic/value", true},
		{"Multi-level wildcard", "topic/#", "topic/level1/level2", true},
		{"Multi-level wildcard root", "#", "any/topic/here", true},
		{"Mixed wildcards", "sensors/+/temperature/#", "sensors/living-room/temperature/celsius", true},
		{"Should not match prefix", "topic", "topic/subtopic", false},
		{"Should not match suffix", "topic/subtopic", "topic", false},
		{"Wildcard level count", "topic/+", "topic/a/b", false},
		{"Empty topic no match", "", "topic", false},
		{"Multi-level no match", "topic/#", "other/topic", false},
		{"Shared subscription", "$share/group1/topic/+", "topic/value", true},
		{"Shared subscription multi-level", "$share/group1/sensors/#", "sensors/temp/value", true},
		{"Shared subscription exact", "$share/group1/exact", "exact", true},
		{"Shared subscription no match", "$share/group1/topic", "other", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sub := NewSubscription(tc.subscribePattern, 0, "client1")
			result := sub.MatchesTopic(tc.publishTopic)

			if result != tc.shouldMatch {
				t.Errorf("Expected match=%v for subscription pattern '%s' and publish topic '%s', got %v",
					tc.shouldMatch, tc.subscribePattern, tc.publishTopic, result)
			}
		})
	}
}

func TestMatchTopicParts(t *testing.T) {
	testCases := []struct {
		name         string
		topicParts   []string
		patternParts []string
		shouldMatch  bool
	}{
		{"Exact match", []string{"a", "b", "c"}, []string{"a", "b", "c"}, true},
		{"Simple wildcard", []string{"a", "b", "c"}, []string{"a", "+", "c"}, true},
		{"Multi-level wildcard", []string{"a", "b", "c", "d"}, []string{"a", "b", "#"}, true},
		{"Multi-level alone", []string{"a", "b", "c"}, []string{"#"}, true},
		{"Not enough parts", []string{"a", "b"}, []string{"a", "b", "c"}, false},
		{"Too many parts", []string{"a", "b", "c"}, []string{"a", "b"}, false},
		{"Multi-level not at end", []string{"a", "b", "c"}, []string{"a", "#", "c"}, false},
		{"Shared subscription", []string{"a", "b", "c"}, []string{"$share", "group1", "a", "b", "c"}, true},
		{"Shared with wildcard", []string{"a", "b", "c", "d"}, []string{"$share", "group1", "a", "+", "c", "d"}, true},
		{"Shared with multi-level", []string{"a", "b", "c", "d"}, []string{"$share", "group1", "a", "b", "#"}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := matchTopicParts(tc.topicParts, tc.patternParts)

			if result != tc.shouldMatch {
				t.Errorf("Expected matchTopicParts(%v, %v) = %v, got %v",
					tc.topicParts, tc.patternParts, tc.shouldMatch, result)
			}
		})
	}
}
