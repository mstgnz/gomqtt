package transform

import (
	"encoding/json"
	"fmt"
	"log"
)

// ExampleConfig returns a sample configuration for the transform plugin
func ExampleConfig() string {
	config := TransformConfig{
		Rules: []TransformRule{
			{
				Name:         "JSON to XML Converter",
				TopicFilter:  "devices/+/data",
				Type:         "transform",
				InputFormat:  "json",
				OutputFormat: "xml",
				Enabled:      true,
			},
			{
				Name:             "Temperature Filter",
				TopicFilter:      "sensors/temperature/#",
				Type:             "filter",
				Condition:        `"temperature":\s*([0-9]+\.[0-9]+)`,
				FilterExpression: `"temperature":\s*(6[0-9]\.[0-9]+|[7-9][0-9]\.[0-9]+|100\.[0-9]+)`, // Filter temp >= 60
				Enabled:          true,
			},
			{
				Name:        "Message Enricher",
				TopicFilter: "events/#",
				Type:        "enrich",
				EnrichmentData: map[string]any{
					"processedBy": "GoMQTT Transform Plugin",
					"timestamp":   "{{timestamp}}",
					"metadata": map[string]any{
						"version": "1.0",
						"source":  "transform-plugin",
					},
				},
				Enabled: true,
			},
			{
				Name:         "JSON Field Mapper",
				TopicFilter:  "data/raw/#",
				Type:         "transform",
				InputFormat:  "json",
				OutputFormat: "json",
				Template:     `{"deviceId": "{{.device_id}}", "readings": {"temp": "{{.temperature}}", "humidity": "{{.humidity}}"}}`,
				Enabled:      true,
			},
		},
	}

	jsonConfig, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("Error marshaling example config: %v", err)
	}

	return fmt.Sprintf(`
# Example Transform Plugin Configuration
# -------------------------------------
# This plugin provides message transformation, filtering, and enrichment capabilities
# Copy this configuration to your config file and adjust as needed

transform:
  %s
`, string(jsonConfig))
}
