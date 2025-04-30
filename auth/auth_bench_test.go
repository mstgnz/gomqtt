package auth

import (
	"testing"
	"time"
)

func BenchmarkGenerateAPIKey(b *testing.B) {
	a := New("test-secret")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = a.GenerateAPIKey(32)
	}
}

func BenchmarkGenerateToken(b *testing.B) {
	a := New("test-secret")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = a.GenerateToken("client1", "user1", time.Hour)
	}
}

func BenchmarkValidateToken(b *testing.B) {
	a := New("test-secret")
	token, _ := a.GenerateToken("client1", "user1", time.Hour)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = a.ValidateToken(token)
	}
}

func BenchmarkCreateAPIKey(b *testing.B) {
	a := New("test-secret")
	_ = a.RegisterUser("benchuser", "password", false)
	permissions := []Permission{
		{TopicPattern: "test/#", AccessLevel: ReadWrite},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Use a different description for each key to avoid conflicts
		_, _ = a.CreateAPIKey("benchuser", "Bench Key "+string(rune(i)), permissions, time.Hour)
	}
}

func BenchmarkValidateAPIKey(b *testing.B) {
	a := New("test-secret")
	_ = a.RegisterUser("benchuser", "password", false)
	permissions := []Permission{
		{TopicPattern: "test/#", AccessLevel: ReadWrite},
	}
	key, _ := a.CreateAPIKey("benchuser", "Bench Key", permissions, time.Hour)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = a.ValidateAPIKey(key)
	}
}

func BenchmarkTopicMatches(b *testing.B) {
	// Run separate benchmarks for different pattern types
	b.Run("ExactMatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = topicMatches("test/topic", "test/topic")
		}
	})

	b.Run("SingleWildcard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = topicMatches("test/+/subtopic", "test/topic/subtopic")
		}
	})

	b.Run("MultiWildcard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = topicMatches("test/#", "test/topic/subtopic/detail")
		}
	})
}

func BenchmarkCheckTopicPermission(b *testing.B) {
	a := New("test-secret")

	// Setup test data
	_ = a.RegisterUser("benchuser", "password", false)
	_ = a.AddUserPermission("benchuser", "test/#", ReadWrite)
	_ = a.RegisterClient("benchclient", "benchuser")

	b.Run("PermittedTopic", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = a.CheckTopicPermission("benchclient", "test/topic", true)
		}
	})

	b.Run("UnpermittedTopic", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = a.CheckTopicPermission("benchclient", "other/topic", true)
		}
	})
}
