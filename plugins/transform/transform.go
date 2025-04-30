package transform

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/mstgnz/gomqtt/plugin"
)

// TransformConfig represents the configuration for the transform plugin
type TransformConfig struct {
	Rules []TransformRule `json:"rules"`
}

// TransformRule defines a transformation rule
type TransformRule struct {
	Name        string `json:"name"`
	TopicFilter string `json:"topic_filter"`
	Condition   string `json:"condition"`
	Type        string `json:"type"` // filter, transform, enrich
	// Filter specific
	FilterExpression string `json:"filter_expression"`
	// Transform specific
	InputFormat  string `json:"input_format"`  // json, xml, text
	OutputFormat string `json:"output_format"` // json, xml, text
	Template     string `json:"template"`
	// Enrich specific
	EnrichmentData map[string]any `json:"enrichment_data"`
	// Common
	Enabled bool `json:"enabled"`
}

// TransformPlugin provides message transformation, filtering and enrichment
type TransformPlugin struct {
	*plugin.BasePlugin
	config *TransformConfig
	rules  []compiledRule
	mutex  sync.RWMutex
	active bool
}

// compiledRule is a TransformRule with pre-compiled expressions
type compiledRule struct {
	rule             TransformRule
	topicMatcher     *regexp.Regexp
	conditionMatcher *regexp.Regexp
	filterMatcher    *regexp.Regexp
}

// NewTransformPlugin creates a new transform plugin
func NewTransformPlugin() *TransformPlugin {
	p := &TransformPlugin{
		BasePlugin: plugin.NewBasePlugin(
			"transform",
			"Transforms, filters, and enriches MQTT messages",
			"1.0.0",
			"GoMQTT Team",
		),
		active: true,
	}

	// Register event handlers
	p.RegisterEventHandler(plugin.EventMessagePublish, p.handlePublish)

	return p
}

// Initialize initializes the transform plugin
func (p *TransformPlugin) Initialize(rawConfig any) error {
	// Parse configuration
	config, ok := rawConfig.(*TransformConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	p.config = config

	// Compile regular expressions for all rules
	p.rules = make([]compiledRule, 0, len(config.Rules))

	for _, rule := range config.Rules {
		if !rule.Enabled {
			continue
		}

		compiledRule, err := p.compileRule(rule)
		if err != nil {
			log.Printf("Failed to compile rule %s: %v", rule.Name, err)
			continue
		}

		p.rules = append(p.rules, compiledRule)
	}

	log.Printf("Transform plugin initialized with %d active rules", len(p.rules))
	return nil
}

// compileRule pre-compiles regular expressions for a rule
func (p *TransformPlugin) compileRule(rule TransformRule) (compiledRule, error) {
	var compiled compiledRule
	var err error

	compiled.rule = rule

	// Convert MQTT wildcards to regex
	topicPattern := rule.TopicFilter
	// Replace + with regex for a single level
	topicPattern = regexp.MustCompile(`\+`).ReplaceAllString(topicPattern, `[^/]+`)
	// Replace # with regex for multiple levels
	topicPattern = regexp.MustCompile(`\#$`).ReplaceAllString(topicPattern, `.*`)
	topicPattern = "^" + topicPattern + "$"

	compiled.topicMatcher, err = regexp.Compile(topicPattern)
	if err != nil {
		return compiled, fmt.Errorf("invalid topic filter pattern: %v", err)
	}

	// Compile condition matcher if present
	if rule.Condition != "" {
		compiled.conditionMatcher, err = regexp.Compile(rule.Condition)
		if err != nil {
			return compiled, fmt.Errorf("invalid condition pattern: %v", err)
		}
	}

	// Compile filter matcher if present
	if rule.Type == "filter" && rule.FilterExpression != "" {
		compiled.filterMatcher, err = regexp.Compile(rule.FilterExpression)
		if err != nil {
			return compiled, fmt.Errorf("invalid filter expression: %v", err)
		}
	}

	return compiled, nil
}

// handlePublish handles message publish events
func (p *TransformPlugin) handlePublish(ctx *plugin.Context) error {
	if !p.active {
		return nil
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Process message through applicable rules
	for _, rule := range p.rules {
		// Check if topic matches
		if !rule.topicMatcher.MatchString(ctx.Topic) {
			continue
		}

		// Check condition if present
		if rule.conditionMatcher != nil {
			if !rule.conditionMatcher.Match(ctx.Payload) {
				continue
			}
		}

		// Apply rule based on type
		switch rule.rule.Type {
		case "filter":
			if p.applyFilter(rule, ctx) {
				// Message passed the filter
				continue
			}
			// Message filtered out
			return fmt.Errorf("message filtered by rule: %s", rule.rule.Name)
		case "transform":
			if err := p.applyTransform(rule, ctx); err != nil {
				log.Printf("Transformation error in rule %s: %v", rule.rule.Name, err)
				// Continue with original message
			}
		case "enrich":
			if err := p.applyEnrichment(rule, ctx); err != nil {
				log.Printf("Enrichment error in rule %s: %v", rule.rule.Name, err)
				// Continue with original message
			}
		}
	}

	return nil
}

// applyFilter returns true if the message passes the filter
func (p *TransformPlugin) applyFilter(rule compiledRule, ctx *plugin.Context) bool {
	if rule.filterMatcher == nil {
		return true
	}

	// Check if the payload matches the filter
	return rule.filterMatcher.Match(ctx.Payload)
}

// applyTransform transforms the message payload
func (p *TransformPlugin) applyTransform(rule compiledRule, ctx *plugin.Context) error {
	if rule.rule.InputFormat == "json" && rule.rule.OutputFormat == "xml" {
		return p.transformJSONToXML(rule, ctx)
	} else if rule.rule.InputFormat == "xml" && rule.rule.OutputFormat == "json" {
		return p.transformXMLToJSON(rule, ctx)
	} else if rule.rule.InputFormat == "json" && rule.rule.OutputFormat == "json" {
		// JSON-to-JSON transformation (e.g., field mapping or restructuring)
		return p.transformJSONToJSON(rule, ctx)
	}

	// Add more transformation types as needed
	return fmt.Errorf("unsupported transformation: %s to %s",
		rule.rule.InputFormat, rule.rule.OutputFormat)
}

// transformJSONToXML converts JSON payload to XML
func (p *TransformPlugin) transformJSONToXML(rule compiledRule, ctx *plugin.Context) error {
	log.Printf("Applying JSON to XML transform using rule: %s", rule.rule.Name)

	// Parse JSON
	var jsonData any
	if err := json.Unmarshal(ctx.Payload, &jsonData); err != nil {
		return fmt.Errorf("error parsing JSON payload: %v", err)
	}

	// Convert to XML
	xmlData, err := xml.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting to XML: %v", err)
	}

	// XML declaration
	xmlOutput := []byte(xml.Header + string(xmlData))

	// Update payload
	ctx.Payload = xmlOutput

	return nil
}

// transformXMLToJSON converts XML payload to JSON
func (p *TransformPlugin) transformXMLToJSON(rule compiledRule, ctx *plugin.Context) error {
	log.Printf("Applying XML to JSON transform using rule: %s", rule.rule.Name)

	// Parse XML
	var xmlData any
	if err := xml.Unmarshal(ctx.Payload, &xmlData); err != nil {
		return fmt.Errorf("error parsing XML payload: %v", err)
	}

	// Convert to JSON
	jsonData, err := json.MarshalIndent(xmlData, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting to JSON: %v", err)
	}

	// Update payload
	ctx.Payload = jsonData

	return nil
}

// transformJSONToJSON transforms a JSON payload to another JSON structure
func (p *TransformPlugin) transformJSONToJSON(rule compiledRule, ctx *plugin.Context) error {
	// Parse input JSON
	var inputData map[string]any
	if err := json.Unmarshal(ctx.Payload, &inputData); err != nil {
		return fmt.Errorf("error parsing JSON payload: %v", err)
	}

	// Apply template if it exists
	if rule.rule.Template != "" {
		// In a real implementation, this would use the template to transform the data
		// For now, we'll just log that we're using the template
		log.Printf("Applying template: %s", rule.rule.Template)
		// Additional template processing would go here
	}

	// Re-encode to JSON
	jsonData, err := json.MarshalIndent(inputData, "", "  ")
	if err != nil {
		return fmt.Errorf("error re-encoding JSON: %v", err)
	}

	// Update payload
	ctx.Payload = jsonData

	return nil
}

// applyEnrichment adds additional data to the message
func (p *TransformPlugin) applyEnrichment(rule compiledRule, ctx *plugin.Context) error {
	// Parse input JSON
	var inputData map[string]any
	if err := json.Unmarshal(ctx.Payload, &inputData); err != nil {
		return fmt.Errorf("error parsing JSON payload: %v", err)
	}

	// Add enrichment data
	for key, value := range rule.rule.EnrichmentData {
		inputData[key] = value
	}

	// Re-encode to JSON
	jsonData, err := json.MarshalIndent(inputData, "", "  ")
	if err != nil {
		return fmt.Errorf("error re-encoding JSON: %v", err)
	}

	// Update payload
	ctx.Payload = jsonData

	return nil
}

// Shutdown stops the transform plugin
func (p *TransformPlugin) Shutdown() error {
	p.mutex.Lock()
	p.active = false
	p.mutex.Unlock()

	log.Printf("Transform plugin shut down")
	return nil
}

// New creates a new transform plugin instance
func New() any {
	return NewTransformPlugin()
}
