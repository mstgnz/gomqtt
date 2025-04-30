package plugin

import (
	"errors"
	"testing"
	"time"
)

func TestNewPlugin(t *testing.T) {
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	if p.Name != "test" {
		t.Errorf("expected name 'test', got '%s'", p.Name)
	}

	if p.Description != "test plugin" {
		t.Errorf("expected description 'test plugin', got '%s'", p.Description)
	}

	if p.Version != "1.0" {
		t.Errorf("expected version '1.0', got '%s'", p.Version)
	}

	if p.Author != "tester" {
		t.Errorf("expected author 'tester', got '%s'", p.Author)
	}

	if len(p.Handlers) != 0 {
		t.Errorf("expected empty handlers map, got %d handlers", len(p.Handlers))
	}
}

func TestOnEvent(t *testing.T) {
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	// Add a handler
	p.OnEvent(EventClientConnect, func(ctx *Context) error {
		return nil
	})

	if len(p.Handlers) != 1 {
		t.Errorf("expected 1 event type, got %d", len(p.Handlers))
	}

	if len(p.Handlers[EventClientConnect]) != 1 {
		t.Errorf("expected 1 handler for event, got %d", len(p.Handlers[EventClientConnect]))
	}

	// Add another handler for same event
	p.OnEvent(EventClientConnect, func(ctx *Context) error {
		return nil
	})

	if len(p.Handlers[EventClientConnect]) != 2 {
		t.Errorf("expected 2 handlers for event, got %d", len(p.Handlers[EventClientConnect]))
	}

	// Add handler for different event
	p.OnEvent(EventMessagePublish, func(ctx *Context) error {
		return nil
	})

	if len(p.Handlers) != 2 {
		t.Errorf("expected 2 event types, got %d", len(p.Handlers))
	}
}

func TestPluginRegistry(t *testing.T) {
	registry := NewPluginRegistry()

	if len(registry.plugins) != 0 {
		t.Errorf("expected empty registry, got %d plugins", len(registry.plugins))
	}
}

func TestRegisterPlugin(t *testing.T) {
	registry := NewPluginRegistry()
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	err := registry.Register(p)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(registry.plugins) != 1 {
		t.Errorf("expected 1 plugin, got %d", len(registry.plugins))
	}

	// Try to register same plugin again
	err = registry.Register(p)
	if err == nil {
		t.Error("expected error when registering duplicate plugin, got nil")
	}
}

func TestUnregisterPlugin(t *testing.T) {
	registry := NewPluginRegistry()
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	registry.Register(p)

	err := registry.Unregister("test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(registry.plugins) != 0 {
		t.Errorf("expected 0 plugins after unregister, got %d", len(registry.plugins))
	}

	// Try to unregister non-existent plugin
	err = registry.Unregister("test")
	if err == nil {
		t.Error("expected error when unregistering non-existent plugin, got nil")
	}
}

func TestTriggerEvent(t *testing.T) {
	registry := NewPluginRegistry()
	p1 := NewPlugin("p1", "plugin 1", "1.0", "tester")
	p2 := NewPlugin("p2", "plugin 2", "1.0", "tester")

	var p1Called, p2Called bool

	p1.OnEvent(EventClientConnect, func(ctx *Context) error {
		p1Called = true
		if ctx.ClientID != "client1" {
			t.Errorf("expected client ID 'client1', got '%s'", ctx.ClientID)
		}
		return nil
	})

	p2.OnEvent(EventClientConnect, func(ctx *Context) error {
		p2Called = true
		return nil
	})

	registry.Register(p1)
	registry.Register(p2)

	ctx := &Context{
		Event:     EventClientConnect,
		ClientID:  "client1",
		Timestamp: time.Now().Unix(),
	}

	errs := registry.TriggerEvent(ctx)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %d", len(errs))
	}

	if !p1Called {
		t.Error("expected p1 handler to be called")
	}

	if !p2Called {
		t.Error("expected p2 handler to be called")
	}
}

func TestTriggerEventWithError(t *testing.T) {
	registry := NewPluginRegistry()
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	expectedErr := errors.New("handler error")

	p.OnEvent(EventClientConnect, func(ctx *Context) error {
		return expectedErr
	})

	registry.Register(p)

	ctx := &Context{
		Event: EventClientConnect,
	}

	errs := registry.TriggerEvent(ctx)
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
}

func TestContextProperties(t *testing.T) {
	ctx := &Context{
		Event:      EventMessagePublish,
		ClientID:   "client1",
		Username:   "user1",
		Topic:      "test/topic",
		Payload:    []byte("hello"),
		Timestamp:  time.Now().Unix(),
		QoS:        1,
		Retained:   true,
		Properties: map[string]any{"key": "value"},
	}

	registry := NewPluginRegistry()
	p := NewPlugin("test", "test plugin", "1.0", "tester")

	p.OnEvent(EventMessagePublish, func(ctx *Context) error {
		if ctx.Topic != "test/topic" {
			t.Errorf("expected topic 'test/topic', got '%s'", ctx.Topic)
		}
		if string(ctx.Payload) != "hello" {
			t.Errorf("expected payload 'hello', got '%s'", string(ctx.Payload))
		}
		if ctx.QoS != 1 {
			t.Errorf("expected QoS 1, got %d", ctx.QoS)
		}
		if !ctx.Retained {
			t.Error("expected retained to be true")
		}
		if val, ok := ctx.Properties["key"]; !ok || val != "value" {
			t.Errorf("expected property 'key' with value 'value', got %v", val)
		}
		return nil
	})

	registry.Register(p)

	errs := registry.TriggerEvent(ctx)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %d", len(errs))
	}
}
