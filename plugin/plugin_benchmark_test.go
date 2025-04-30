package plugin

import (
	"testing"
	"time"
)

func BenchmarkRegisterPlugin(b *testing.B) {
	registry := NewPluginRegistry()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewPlugin("test"+string(rune(i)), "test plugin", "1.0", "tester")
		registry.Register(p)
	}
}

func BenchmarkUnregisterPlugin(b *testing.B) {
	registry := NewPluginRegistry()

	// Register plugins first
	for i := 0; i < b.N; i++ {
		name := "test" + string(rune(i))
		p := NewPlugin(name, "test plugin", "1.0", "tester")
		registry.Register(p)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := "test" + string(rune(i))
		registry.Unregister(name)
	}
}

func BenchmarkTriggerEvent(b *testing.B) {
	registry := NewPluginRegistry()

	// Register a plugin with a handler
	p := NewPlugin("test", "test plugin", "1.0", "tester")
	p.OnEvent(EventClientConnect, func(ctx *Context) error {
		return nil
	})
	registry.Register(p)

	ctx := &Context{
		Event:     EventClientConnect,
		ClientID:  "client1",
		Timestamp: time.Now().Unix(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.TriggerEvent(ctx)
	}
}

func BenchmarkTriggerEventMultiplePlugins(b *testing.B) {
	registry := NewPluginRegistry()

	// Register 10 plugins with handlers
	for i := 0; i < 10; i++ {
		p := NewPlugin("test"+string(rune(i)), "test plugin", "1.0", "tester")
		p.OnEvent(EventClientConnect, func(ctx *Context) error {
			return nil
		})
		registry.Register(p)
	}

	ctx := &Context{
		Event:     EventClientConnect,
		ClientID:  "client1",
		Timestamp: time.Now().Unix(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.TriggerEvent(ctx)
	}
}

func BenchmarkTriggerEventMultipleHandlers(b *testing.B) {
	registry := NewPluginRegistry()
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	// Add 10 handlers for the same event
	for i := 0; i < 10; i++ {
		p.OnEvent(EventClientConnect, func(ctx *Context) error {
			return nil
		})
	}
	registry.Register(p)

	ctx := &Context{
		Event:     EventClientConnect,
		ClientID:  "client1",
		Timestamp: time.Now().Unix(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.TriggerEvent(ctx)
	}
}

func BenchmarkNewPlugin(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewPlugin("test", "test plugin", "1.0", "tester")
	}
}

func BenchmarkOnEvent(b *testing.B) {
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	handler := func(ctx *Context) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.OnEvent(EventClientConnect, handler)
	}
}
