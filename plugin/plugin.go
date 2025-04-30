package plugin

import (
	"fmt"
	"sync"
)

// Event types for the plugin system
const (
	EventClientConnect    = "client.connect"
	EventClientDisconnect = "client.disconnect"
	EventMessagePublish   = "message.publish"
	EventMessageReceive   = "message.receive"
	EventSubscribe        = "client.subscribe"
	EventUnsubscribe      = "client.unsubscribe"
)

// Context is passed to plugins when events are triggered
type Context struct {
	Event      string
	ClientID   string
	Username   string
	Topic      string
	Payload    []byte
	Timestamp  int64
	QoS        byte
	Retained   bool
	Properties map[string]any
}

// Handler is a function that processes events
type Handler func(ctx *Context) error

// Plugin represents a broker plugin
type Plugin struct {
	Name        string
	Description string
	Version     string
	Author      string
	Handlers    map[string][]Handler
}

// PluginRegistry manages the plugins
type PluginRegistry struct {
	plugins map[string]*Plugin
	mutex   sync.RWMutex
}

// NewPluginRegistry creates a new plugin registry
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		plugins: make(map[string]*Plugin),
	}
}

// Register registers a plugin
func (r *PluginRegistry) Register(p *Plugin) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.plugins[p.Name]; exists {
		return fmt.Errorf("plugin %s already registered", p.Name)
	}

	r.plugins[p.Name] = p
	return nil
}

// Unregister removes a plugin
func (r *PluginRegistry) Unregister(name string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.plugins[name]; !exists {
		return fmt.Errorf("plugin %s not registered", name)
	}

	delete(r.plugins, name)
	return nil
}

// TriggerEvent triggers an event on all registered plugins
func (r *PluginRegistry) TriggerEvent(ctx *Context) []error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var errors []error

	for _, p := range r.plugins {
		if handlers, ok := p.Handlers[ctx.Event]; ok {
			for _, handler := range handlers {
				if err := handler(ctx); err != nil {
					errors = append(errors, fmt.Errorf("plugin %s error: %w", p.Name, err))
				}
			}
		}
	}

	return errors
}

// NewPlugin creates a new plugin
func NewPlugin(name, description, version, author string) *Plugin {
	return &Plugin{
		Name:        name,
		Description: description,
		Version:     version,
		Author:      author,
		Handlers:    make(map[string][]Handler),
	}
}

// OnEvent registers a handler for an event
func (p *Plugin) OnEvent(event string, handler Handler) {
	if p.Handlers == nil {
		p.Handlers = make(map[string][]Handler)
	}

	p.Handlers[event] = append(p.Handlers[event], handler)
}
