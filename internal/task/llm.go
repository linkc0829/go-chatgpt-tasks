package task

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type LLMRequest struct {
	Model           string
	Prompt          string
	MaxInputTokens  int
	MaxOutputTokens int
}

type LLMResponse struct {
	Content      string
	InputTokens  int
	OutputTokens int
}

type LLMPolicy struct {
	TimeoutSeconds  int
	MaxRetries      int
	MaxInputTokens  int
	MaxOutputTokens int
	MaxCostCents    int
	OutputSchema    string
}

// ValidateOutput validates a response against a practical JSON Schema subset:
// type, required, properties, and items. A plain object remains supported as a
// shorthand where every key is required and non-null values define types.
func ValidateOutput(schema, content string) error {
	if schema == "" {
		return nil
	}
	var configured any
	if err := json.Unmarshal([]byte(schema), &configured); err != nil {
		return fmt.Errorf("%w: invalid configured schema", ErrInvalidLLMOutput)
	}
	var got any
	if err := json.Unmarshal([]byte(content), &got); err != nil {
		return fmt.Errorf("%w: response is not valid JSON", ErrInvalidLLMOutput)
	}
	if err := validateJSONValue(configured, got, "$"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLLMOutput, err)
	}
	return nil
}

func validateJSONValue(schema, value any, path string) error {
	object, ok := schema.(map[string]any)
	if !ok {
		if schema != nil && reflect.TypeOf(schema) != reflect.TypeOf(value) {
			return fmt.Errorf("%s has wrong type", path)
		}
		return nil
	}

	if schemaType, _ := object["type"].(string); schemaType != "" {
		if !matchesJSONType(schemaType, value) {
			return fmt.Errorf("%s must be %s", path, schemaType)
		}
		if schemaType == "array" {
			if itemSchema, ok := object["items"]; ok {
				for i, item := range value.([]any) {
					if err := validateJSONValue(itemSchema, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if schemaType != "object" {
			return nil
		}
	}

	got, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be object", path)
	}
	properties, hasProperties := object["properties"].(map[string]any)
	if !hasProperties {
		properties = object
	}
	required := requiredFields(object, properties, hasProperties)
	for _, field := range required {
		if _, ok := got[field]; !ok {
			return fmt.Errorf("%s missing field %q", path, field)
		}
	}
	for field, fieldSchema := range properties {
		fieldValue, ok := got[field]
		if !ok {
			continue
		}
		if err := validateJSONValue(fieldSchema, fieldValue, path+"."+field); err != nil {
			return err
		}
	}
	return nil
}

func requiredFields(schema, properties map[string]any, explicit bool) []string {
	if raw, ok := schema["required"].([]any); ok {
		out := make([]string, 0, len(raw))
		for _, field := range raw {
			if name, ok := field.(string); ok {
				out = append(out, name)
			}
		}
		return out
	}
	if explicit {
		return nil
	}
	out := make([]string, 0, len(properties))
	for field := range properties {
		if field != "type" && field != "required" && field != "properties" && field != "items" {
			out = append(out, field)
		}
	}
	return out
}

func matchesJSONType(schemaType string, value any) bool {
	switch schemaType {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "integer":
		n, ok := value.(float64)
		return ok && n == float64(int64(n))
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return false
	}
}

func EstimateCostCents(_ string, inputTokens, outputTokens int) int {
	total := inputTokens + outputTokens
	if total <= 0 {
		return 0
	}
	return (total + 999) / 1000
}
