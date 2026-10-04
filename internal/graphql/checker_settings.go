package graphql

import (
	"fmt"
	gql "github.com/graphql-go/graphql"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"strconv"
)

func checkerSettingsTypes() (*gql.Object, *gql.InputObject) {
	protocols := gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String)))
	profile := gql.NewObject(gql.ObjectConfig{Name: "CheckerProfileSettings", Fields: gql.Fields{
		"protocols": {Type: protocols}, "transport": {Type: gql.String}, "timeout": {Type: gql.Int}, "retries": {Type: gql.Int},
	}})
	rule := gql.NewObject(gql.ObjectConfig{Name: "TagCheckerRule", Fields: gql.Fields{
		"tagId": {Type: gql.NewNonNull(gql.ID)}, "mode": {Type: gql.NewNonNull(gql.String)}, "protocols": {Type: protocols}, "transport": {Type: gql.String}, "timeout": {Type: gql.Int}, "retries": {Type: gql.Int},
	}})
	output := gql.NewObject(gql.ObjectConfig{Name: "CheckerSettings", Fields: gql.Fields{"defaults": {Type: profile}, "rules": {Type: gql.NewList(rule)}}})
	profileInput := gql.NewInputObject(gql.InputObjectConfig{Name: "CheckerProfileInput", Fields: gql.InputObjectConfigFieldMap{
		"protocols": {Type: protocols}, "transport": {Type: gql.NewNonNull(gql.String)}, "timeout": {Type: gql.NewNonNull(gql.Int)}, "retries": {Type: gql.NewNonNull(gql.Int)},
	}})
	ruleInput := gql.NewInputObject(gql.InputObjectConfig{Name: "TagCheckerRuleInput", Fields: gql.InputObjectConfigFieldMap{
		"tagId": {Type: gql.NewNonNull(gql.ID)}, "mode": {Type: gql.NewNonNull(gql.String)}, "protocols": {Type: protocols}, "transport": {Type: gql.String}, "timeout": {Type: gql.Int}, "retries": {Type: gql.Int},
	}})
	input := gql.NewInputObject(gql.InputObjectConfig{Name: "CheckerSettingsInput", Fields: gql.InputObjectConfigFieldMap{
		"defaults": {Type: gql.NewNonNull(profileInput)}, "rules": {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(ruleInput)))},
	}})
	return output, input
}
func checkerSettingsOutput(value *dto.CheckerSettings) map[string]any {
	defaults := value.Defaults
	rules := make([]map[string]any, 0, len(value.Rules))
	for _, rule := range value.Rules {
		entry := map[string]any{"tagId": strconv.FormatUint(rule.TagID, 10), "mode": rule.Mode, "protocols": rule.Protocols}
		if rule.Transport != nil {
			entry["transport"] = *rule.Transport
		}
		if rule.Timeout != nil {
			entry["timeout"] = int(*rule.Timeout)
		}
		if rule.Retries != nil {
			entry["retries"] = int(*rule.Retries)
		}
		rules = append(rules, entry)
	}
	return map[string]any{"defaults": map[string]any{"protocols": defaults.Protocols, "transport": defaults.Transport, "timeout": int(defaults.Timeout), "retries": int(defaults.Retries)}, "rules": rules}
}
func parseCheckerSettings(raw map[string]any) (*dto.CheckerSettings, error) {
	parse := func(entry map[string]any) (dto.TagCheckerRule, error) {
		v := dto.TagCheckerRule{Protocols: []string{}}
		protocols, _ := entry["protocols"].([]any)
		for _, item := range protocols {
			name, _ := item.(string)
			v.Protocols = append(v.Protocols, name)
		}
		if transport, ok := entry["transport"].(string); ok {
			v.Transport = &transport
		}
		if timeout, ok := entry["timeout"].(int); ok {
			if timeout < 1 || timeout > 65535 {
				return v, fmt.Errorf("timeout must be between 1 and 65535")
			}
			n := uint16(timeout)
			v.Timeout = &n
		}
		if retries, ok := entry["retries"].(int); ok {
			if retries < 0 || retries > 255 {
				return v, fmt.Errorf("retries must be between 0 and 255")
			}
			n := uint8(retries)
			v.Retries = &n
		}
		return v, nil
	}
	defaults, _ := raw["defaults"].(map[string]any)
	fields, err := parse(defaults)
	if err != nil {
		return nil, err
	}
	if fields.Transport == nil || fields.Timeout == nil || fields.Retries == nil {
		return nil, fmt.Errorf("defaults require transport, timeout and retries")
	}
	value := &dto.CheckerSettings{Defaults: dto.CheckerProfileSettings{Protocols: fields.Protocols, Transport: *fields.Transport, Timeout: *fields.Timeout, Retries: *fields.Retries}, Rules: []dto.TagCheckerRule{}}
	rules, _ := raw["rules"].([]any)
	for _, item := range rules {
		entry, _ := item.(map[string]any)
		rule, err := parse(entry)
		if err != nil {
			return nil, err
		}
		rule.TagID, err = strconv.ParseUint(fmt.Sprint(entry["tagId"]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid checker tag ID")
		}
		rule.Mode, _ = entry["mode"].(string)
		value.Rules = append(value.Rules, rule)
	}
	return value, domain.ValidateCheckerSettings(value)
}
