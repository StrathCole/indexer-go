package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type swaggerDocCache struct {
	mu        sync.Mutex
	cachedAt  time.Time
	doc       []byte
	lastError error
}

const swaggerCacheTTL = 5 * time.Minute

var (
	fcdSwaggerOnce sync.Once
	fcdSwaggerDoc  []byte
	fcdSwaggerErr  error
)

func (s *Server) getSwaggerDoc(ctx context.Context) ([]byte, error) {
	fcdSwaggerOnce.Do(func() {
		// Validate the embedded JSON and normalize formatting once.
		var tmp any
		if err := json.Unmarshal(swaggerFCDSpecJSON, &tmp); err != nil {
			fcdSwaggerErr = err
			return
		}
		fcdSwaggerDoc = swaggerFCDSpecJSON
	})
	if fcdSwaggerErr != nil {
		return nil, fcdSwaggerErr
	}

	lcdURL := strings.TrimSpace(s.lcdURL)
	if lcdURL == "" {
		return fcdSwaggerDoc, nil
	}

	// Simple TTL cache to avoid fetching/parsing on every request.
	s.swaggerCache.mu.Lock()
	if len(s.swaggerCache.doc) > 0 && time.Since(s.swaggerCache.cachedAt) < swaggerCacheTTL {
		doc := append([]byte(nil), s.swaggerCache.doc...)
		s.swaggerCache.mu.Unlock()
		return doc, nil
	}
	s.swaggerCache.mu.Unlock()

	merged, err := buildMergedSwaggerDoc(ctx, lcdURL)
	s.swaggerCache.mu.Lock()
	s.swaggerCache.cachedAt = time.Now()
	s.swaggerCache.doc = merged
	s.swaggerCache.lastError = err
	s.swaggerCache.mu.Unlock()

	if err != nil {
		// Fallback to local-only when LCD fetch/merge fails.
		return fcdSwaggerDoc, nil
	}
	return merged, nil
}

func buildMergedSwaggerDoc(ctx context.Context, lcdBase string) ([]byte, error) {
	fcdSpec, err := parseJSONToMap(swaggerFCDSpecJSON)
	if err != nil {
		return nil, err
	}

	lcdSpecBytes, err := fetchLCDSwagger(ctx, lcdBase)
	if err != nil {
		return nil, err
	}

	lcdSpec, err := parseYAMLOrJSONToMap(lcdSpecBytes)
	if err != nil {
		return nil, err
	}

	if v, _ := fcdSpec["swagger"].(string); v == "" {
		return nil, errors.New("local swagger spec missing 'swagger' version")
	}

	mergeSwaggerIntoBase(fcdSpec, lcdSpec)

	out, err := json.MarshalIndent(fcdSpec, "", "  ")
	if err != nil {
		return nil, err
	}
	return out, nil
}

func fetchLCDSwagger(ctx context.Context, lcdBase string) ([]byte, error) {
	base := strings.TrimRight(strings.TrimSpace(lcdBase), "/")
	if base == "" {
		return nil, errors.New("empty lcd url")
	}

	swaggerURL := base
	if !strings.HasSuffix(swaggerURL, ".yaml") && !strings.HasSuffix(swaggerURL, ".yml") && !strings.HasSuffix(swaggerURL, ".json") {
		swaggerURL = swaggerURL + "/swagger/swagger.yaml"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, swaggerURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("lcd swagger returned non-2xx")
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20)) // 5MB cap
	if err != nil {
		return nil, err
	}
	return body, nil
}

func parseJSONToMap(b []byte) (map[string]any, error) {
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func parseYAMLOrJSONToMap(b []byte) (map[string]any, error) {
	trim := bytes.TrimSpace(b)
	if len(trim) == 0 {
		return nil, errors.New("empty swagger document")
	}
	if trim[0] == '{' {
		return parseJSONToMap(trim)
	}

	var y any
	if err := yaml.Unmarshal(trim, &y); err != nil {
		return nil, err
	}

	n := normalizeYAML(y)
	m, ok := n.(map[string]any)
	if !ok {
		return nil, errors.New("unexpected swagger yaml root")
	}
	return m, nil
}

func normalizeYAML(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, v2 := range t {
			out[k] = normalizeYAML(v2)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, v2 := range t {
			ks, ok := k.(string)
			if !ok {
				continue
			}
			out[ks] = normalizeYAML(v2)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, item := range t {
			out = append(out, normalizeYAML(item))
		}
		return out
	default:
		return v
	}
}

func mergeSwaggerIntoBase(base map[string]any, other map[string]any) {
	mergeMapKeyUnion(base, other, "definitions")
	mergeMapKeyUnion(base, other, "parameters")
	mergeMapKeyUnion(base, other, "responses")
	mergeMapKeyUnion(base, other, "securityDefinitions")

	mergePaths(base, other)
	mergeTags(base, other)
}

func mergePaths(base map[string]any, other map[string]any) {
	basePaths, _ := base["paths"].(map[string]any)
	otherPaths, _ := other["paths"].(map[string]any)
	if otherPaths == nil {
		return
	}
	if basePaths == nil {
		basePaths = map[string]any{}
		base["paths"] = basePaths
	}

	for p, v := range otherPaths {
		if _, exists := basePaths[p]; exists {
			continue
		}
		basePaths[p] = v
	}
}

func mergeTags(base map[string]any, other map[string]any) {
	baseTags, _ := base["tags"].([]any)
	otherTags, _ := other["tags"].([]any)
	if len(otherTags) == 0 {
		return
	}

	seen := map[string]struct{}{}
	for _, t := range baseTags {
		if m, ok := t.(map[string]any); ok {
			if name, _ := m["name"].(string); name != "" {
				seen[name] = struct{}{}
			}
		}
	}

	merged := make([]any, 0, len(baseTags)+len(otherTags))
	merged = append(merged, baseTags...)
	for _, t := range otherTags {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		merged = append(merged, t)
	}

	base["tags"] = merged
}

func mergeMapKeyUnion(base map[string]any, other map[string]any, key string) {
	otherMap, _ := other[key].(map[string]any)
	if otherMap == nil {
		return
	}

	baseMap, _ := base[key].(map[string]any)
	if baseMap == nil {
		baseMap = map[string]any{}
		base[key] = baseMap
	}

	for k, v := range otherMap {
		if _, exists := baseMap[k]; exists {
			continue
		}
		baseMap[k] = v
	}
}
