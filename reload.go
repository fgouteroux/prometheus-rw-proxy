package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

// TenantConfigReloader handles tenant configuration reloading on SIGHUP only
type TenantConfigReloader struct {
	configPath string
	processor  *processor
	mu         sync.RWMutex
}

// NewTenantConfigReloader creates a new tenant config reloader
func NewTenantConfigReloader(configPath string, processor *processor) *TenantConfigReloader {
	return &TenantConfigReloader{
		configPath: configPath,
		processor:  processor,
	}
}

// ReloadTenantConfig performs tenant configuration reload from file
func (cr *TenantConfigReloader) ReloadTenantConfig() error {
	if cr.configPath == "" {
		return fmt.Errorf("no config file specified")
	}

	log.Info("Reloading tenant configuration from file...")

	// Load the new configuration
	newCfg, err := configLoad(cr.configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	// Validate the new tenant configuration
	if err := cr.validateTenantConfig(&newCfg.Tenant); err != nil {
		return fmt.Errorf("tenant config validation failed: %v", err)
	}

	// Apply the new tenant configuration
	cr.mu.Lock()
	defer cr.mu.Unlock()

	oldTenantCfg := cr.processor.cfg.Tenant
	if err := cr.applyTenantConfig(&newCfg.Tenant); err != nil {
		return fmt.Errorf("failed to apply new tenant config: %v", err)
	}

	log.Info("Tenant configuration reloaded successfully")
	cr.logTenantConfigChanges(&oldTenantCfg, &newCfg.Tenant)

	return nil
}

// validateTenantConfig validates the new tenant configuration
func (cr *TenantConfigReloader) validateTenantConfig(tenantCfg *struct {
	Label              string         `yaml:"label"`
	LabelList          []string       `yaml:"label_list"`
	Prefix             string         `yaml:"prefix"`
	PrefixPreferSource bool           `yaml:"prefix_prefer_source"`
	LabelRemove        bool           `yaml:"label_remove"`
	Header             string         `yaml:"header"`
	Default            string         `yaml:"default"`
	DispatchAll        string         `yaml:"dispatch_all"`
	AcceptAll          bool           `yaml:"accept_all"`
	LabelValueMatcher  []LabelMatcher `yaml:"label_value_matcher"`
	AllowList          []string       `yaml:"allow_list"`
}) error {
	// Validate tenant label list is not empty
	if len(tenantCfg.LabelList) == 0 && tenantCfg.Label == "" {
		return fmt.Errorf("tenant label or label_list must be specified")
	}

	// Validate regex patterns if provided
	for _, matcher := range tenantCfg.LabelValueMatcher {
		if _, err := regexp.Compile(matcher.RegexStr); err != nil {
			return fmt.Errorf("invalid regex for '%s': %v", matcher.Name, err)
		}
	}

	return nil
}

// applyTenantConfig applies the new tenant configuration to the processor
func (cr *TenantConfigReloader) applyTenantConfig(newTenantCfg *struct {
	Label              string         `yaml:"label"`
	LabelList          []string       `yaml:"label_list"`
	Prefix             string         `yaml:"prefix"`
	PrefixPreferSource bool           `yaml:"prefix_prefer_source"`
	LabelRemove        bool           `yaml:"label_remove"`
	Header             string         `yaml:"header"`
	Default            string         `yaml:"default"`
	DispatchAll        string         `yaml:"dispatch_all"`
	AcceptAll          bool           `yaml:"accept_all"`
	LabelValueMatcher  []LabelMatcher `yaml:"label_value_matcher"`
	AllowList          []string       `yaml:"allow_list"`
}) error {
	// Log current state before changes
	log.Debugf("Before applying tenant config - current LabelValueRegex count: %d", len(LabelValueRegex))
	for i, matcher := range LabelValueRegex {
		log.Debugf("  [%d] %s: %s", i, matcher.Name, matcher.Regex.String())
	}

	// Update processor tenant configuration
	cr.processor.cfg.Tenant.Label = newTenantCfg.Label
	cr.processor.cfg.Tenant.LabelList = newTenantCfg.LabelList
	cr.processor.cfg.Tenant.Prefix = newTenantCfg.Prefix
	cr.processor.cfg.Tenant.PrefixPreferSource = newTenantCfg.PrefixPreferSource
	cr.processor.cfg.Tenant.LabelRemove = newTenantCfg.LabelRemove
	cr.processor.cfg.Tenant.Header = newTenantCfg.Header
	cr.processor.cfg.Tenant.Default = newTenantCfg.Default
	cr.processor.cfg.Tenant.AcceptAll = newTenantCfg.AcceptAll
	cr.processor.cfg.Tenant.LabelValueMatcher = newTenantCfg.LabelValueMatcher
	cr.processor.cfg.Tenant.AllowList = newTenantCfg.AllowList

	// Recompile label value regex patterns
	log.Debugf("Recompiling %d label value regex patterns...", len(newTenantCfg.LabelValueMatcher))
	LabelValueRegex = nil

	for i, matcher := range newTenantCfg.LabelValueMatcher {
		log.Debugf("Compiling matcher [%d]: %s -> %s", i, matcher.Name, matcher.RegexStr)
		regex, err := regexp.Compile(matcher.RegexStr)
		if err != nil {
			log.Errorf("Failed to compile regex for '%s': %v", matcher.Name, err)
			return fmt.Errorf("invalid regex for '%s': %v", matcher.Name, err)
		}

		LabelValueRegex = append(LabelValueRegex, CompiledMatcher{
			Name:  matcher.Name,
			Regex: regex,
		})
		log.Debugf("Successfully compiled matcher: %s", matcher.Name)
	}

	// Log final state after changes
	log.Debugf("After applying tenant config - new LabelValueRegex count: %d", len(LabelValueRegex))
	for i, matcher := range LabelValueRegex {
		log.Debugf("  Active matcher [%d]: %s -> %s", i, matcher.Name, matcher.Regex.String())
	}

	return nil
}

// logTenantConfigChanges logs the differences between old and new tenant configurations
func (cr *TenantConfigReloader) logTenantConfigChanges(oldCfg, newCfg *struct {
	Label              string         `yaml:"label"`
	LabelList          []string       `yaml:"label_list"`
	Prefix             string         `yaml:"prefix"`
	PrefixPreferSource bool           `yaml:"prefix_prefer_source"`
	LabelRemove        bool           `yaml:"label_remove"`
	Header             string         `yaml:"header"`
	Default            string         `yaml:"default"`
	DispatchAll        string         `yaml:"dispatch_all"`
	AcceptAll          bool           `yaml:"accept_all"`
	LabelValueMatcher  []LabelMatcher `yaml:"label_value_matcher"`
	AllowList          []string       `yaml:"allow_list"`
}) {
	changes := []string{}

	if oldCfg.Label != newCfg.Label {
		changes = append(changes, fmt.Sprintf("label: %s -> %s", oldCfg.Label, newCfg.Label))
	}

	if !stringSlicesEqual(oldCfg.LabelList, newCfg.LabelList) {
		changes = append(changes, fmt.Sprintf("label_list: %v -> %v", oldCfg.LabelList, newCfg.LabelList))
	}

	if oldCfg.Prefix != newCfg.Prefix {
		changes = append(changes, fmt.Sprintf("prefix: %s -> %s", oldCfg.Prefix, newCfg.Prefix))
	}

	if oldCfg.PrefixPreferSource != newCfg.PrefixPreferSource {
		changes = append(changes, fmt.Sprintf("prefix_prefer_source: %t -> %t", oldCfg.PrefixPreferSource, newCfg.PrefixPreferSource))
	}

	if oldCfg.LabelRemove != newCfg.LabelRemove {
		changes = append(changes, fmt.Sprintf("label_remove: %t -> %t", oldCfg.LabelRemove, newCfg.LabelRemove))
	}

	if oldCfg.Header != newCfg.Header {
		changes = append(changes, fmt.Sprintf("header: %s -> %s", oldCfg.Header, newCfg.Header))
	}

	if oldCfg.Default != newCfg.Default {
		changes = append(changes, fmt.Sprintf("default: %s -> %s", oldCfg.Default, newCfg.Default))
	}

	if oldCfg.DispatchAll != newCfg.DispatchAll {
		changes = append(changes, fmt.Sprintf("dispatch_all: %s -> %s", oldCfg.DispatchAll, newCfg.DispatchAll))
	}

	if oldCfg.AcceptAll != newCfg.AcceptAll {
		changes = append(changes, fmt.Sprintf("accept_all: %t -> %t", oldCfg.AcceptAll, newCfg.AcceptAll))
	}

	if !stringSlicesEqual(oldCfg.AllowList, newCfg.AllowList) {
		changes = append(changes, fmt.Sprintf("allow_list: %v -> %v", oldCfg.AllowList, newCfg.AllowList))
	}

	// Detailed logging for label value matchers
	if !labelMatchersEqual(oldCfg.LabelValueMatcher, newCfg.LabelValueMatcher) {
		changes = append(changes, "label_value_matcher: updated")

		// Log detailed changes for label value matchers
		cr.logLabelMatcherChanges(oldCfg.LabelValueMatcher, newCfg.LabelValueMatcher)
	}

	if len(changes) > 0 {
		log.Infof("Tenant configuration changes detected: %s", strings.Join(changes, ", "))
	} else {
		log.Info("Tenant configuration reloaded with no functional changes")
	}
}

// logLabelMatcherChanges provides detailed logging of label matcher changes
func (cr *TenantConfigReloader) logLabelMatcherChanges(oldMatchers, newMatchers []LabelMatcher) {
	log.Info("=== Label Value Matcher Changes ===")

	// Create maps for easier comparison
	oldMap := make(map[string]string)
	newMap := make(map[string]string)

	for _, matcher := range oldMatchers {
		oldMap[matcher.Name] = matcher.RegexStr
	}

	for _, matcher := range newMatchers {
		newMap[matcher.Name] = matcher.RegexStr
	}

	// Check for removed matchers
	for name, regex := range oldMap {
		if _, exists := newMap[name]; !exists {
			log.Warnf("  REMOVED matcher: %s (regex: %s)", name, regex)
		}
	}

	// Check for added matchers
	for name, regex := range newMap {
		if _, exists := oldMap[name]; !exists {
			log.Infof("  ADDED matcher: %s (regex: %s)", name, regex)
		}
	}

	// Check for modified matchers
	for name, newRegex := range newMap {
		if oldRegex, exists := oldMap[name]; exists && oldRegex != newRegex {
			log.Infof("  MODIFIED matcher: %s", name)
			log.Infof("    OLD regex: %s", oldRegex)
			log.Infof("    NEW regex: %s", newRegex)
		}
	}

	// Log summary
	log.Infof("Label matcher summary: %d old -> %d new matchers", len(oldMatchers), len(newMatchers))

	// Log the final compiled matchers
	if len(newMatchers) > 0 {
		log.Debug("Active label value matchers after reload:")
		for _, matcher := range newMatchers {
			log.Debugf("  - %s: %s", matcher.Name, matcher.RegexStr)
		}
	} else {
		log.Info("No label value matchers configured after reload")
	}

	log.Info("=== End Label Value Matcher Changes ===")
}

// Helper functions for comparing slices and structs
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func labelMatchersEqual(a, b []LabelMatcher) bool {
	if len(a) != len(b) {
		log.Debugf("Label matchers length differs: %d vs %d", len(a), len(b))
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			log.Debugf("Label matcher name differs at index %d: '%s' vs '%s'", i, a[i].Name, b[i].Name)
			return false
		}
		if a[i].RegexStr != b[i].RegexStr {
			log.Debugf("Label matcher regex differs at index %d for '%s': '%s' vs '%s'", i, a[i].Name, a[i].RegexStr, b[i].RegexStr)
			return false
		}
	}
	log.Debugf("Label matchers are equal (%d matchers)", len(a))
	return true
}
