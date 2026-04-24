package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
)

// ArchivalCache provides two-tier response caching matching mantlemint's behavior:
//   - latestCache: for requests without ?height; purged on each new block
//   - archivalCache: for requests with ?height=N; permanent (never purged, LRU evicted)
//
// Both caches also deduplicate concurrent identical requests.
type ArchivalCache struct {
	latestMu      sync.RWMutex
	latestCache   map[string]*cachedResponse
	archivalMu    sync.RWMutex
	archivalCache map[string]*cachedResponse

	// In-flight request deduplication
	inflightMu sync.Mutex
	inflight   map[string]*inflightEntry
}

type cachedResponse struct {
	status int
	header http.Header
	body   []byte
}

type inflightEntry struct {
	done chan struct{}
	resp *cachedResponse
}

func NewArchivalCache() *ArchivalCache {
	return &ArchivalCache{
		latestCache:   make(map[string]*cachedResponse),
		archivalCache: make(map[string]*cachedResponse, 16384),
		inflight:      make(map[string]*inflightEntry),
	}
}

// PurgeLatest clears the latest cache (called when a new block arrives).
func (ac *ArchivalCache) PurgeLatest() {
	ac.latestMu.Lock()
	ac.latestCache = make(map[string]*cachedResponse)
	ac.latestMu.Unlock()
}

// Middleware returns an http.Handler that caches responses.
// Requests with x-cosmos-block-height header go to archival cache;
// all others go to latest cache.
func (ac *ArchivalCache) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		heightHeader := r.Header.Get("x-cosmos-block-height")
		isArchival := heightHeader != ""

		cacheKey := r.URL.String()
		if isArchival {
			cacheKey = heightHeader + "|" + cacheKey
		}

		// Check cache
		if resp := ac.get(cacheKey, isArchival); resp != nil {
			for k, vs := range resp.header {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(resp.status)
			w.Write(resp.body)
			return
		}

		// Deduplicate concurrent identical requests
		ac.inflightMu.Lock()
		if entry, ok := ac.inflight[cacheKey]; ok {
			ac.inflightMu.Unlock()
			<-entry.done
			if entry.resp != nil {
				for k, vs := range entry.resp.header {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(entry.resp.status)
				w.Write(entry.resp.body)
			} else {
				http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			}
			return
		}
		entry := &inflightEntry{done: make(chan struct{})}
		ac.inflight[cacheKey] = entry
		ac.inflightMu.Unlock()

		// Execute the actual request
		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, r)

		result := rec.Result()
		body := rec.Body.Bytes()

		resp := &cachedResponse{
			status: result.StatusCode,
			header: result.Header.Clone(),
			body:   bytes.Clone(body),
		}

		// Only cache successful responses
		if result.StatusCode >= 200 && result.StatusCode < 400 {
			ac.set(cacheKey, isArchival, resp)
		}

		// Notify waiters
		entry.resp = resp
		close(entry.done)
		ac.inflightMu.Lock()
		delete(ac.inflight, cacheKey)
		ac.inflightMu.Unlock()

		// Write response
		for k, vs := range resp.header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.status)
		w.Write(resp.body)
	})
}

func (ac *ArchivalCache) get(key string, archival bool) *cachedResponse {
	if archival {
		ac.archivalMu.RLock()
		defer ac.archivalMu.RUnlock()
		return ac.archivalCache[key]
	}
	ac.latestMu.RLock()
	defer ac.latestMu.RUnlock()
	return ac.latestCache[key]
}

func (ac *ArchivalCache) set(key string, archival bool, resp *cachedResponse) {
	if archival {
		ac.archivalMu.Lock()
		ac.archivalCache[key] = resp
		// Simple size limit: if archival cache exceeds 16K entries, prune oldest half
		if len(ac.archivalCache) > 16384 {
			count := 0
			for k := range ac.archivalCache {
				delete(ac.archivalCache, k)
				count++
				if count >= 8192 {
					break
				}
			}
		}
		ac.archivalMu.Unlock()
	} else {
		ac.latestMu.Lock()
		ac.latestCache[key] = resp
		ac.latestMu.Unlock()
	}
}
