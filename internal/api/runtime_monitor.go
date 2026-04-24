package api

import (
	"context"
	"time"

	"sync/atomic"

	"github.com/rs/zerolog/log"
)

const (
	runtimePollInterval     = 2 * time.Second
	runtimeHealthStaleAfter = 15 * time.Second
	runtimeSyncLagTolerance = uint64(1)
)

type runtimeStatus struct {
	nodeHeight      atomic.Uint64
	indexedHeight   atomic.Uint64
	lastCheckedUnix atomic.Int64
	synced          atomic.Bool
}

func newRuntimeStatus() *runtimeStatus {
	rs := &runtimeStatus{}
	rs.synced.Store(false)
	return rs
}

func (s *Server) startRuntimeMonitor() {
	if s == nil || s.runtimeStatus == nil || s.rpc == nil || s.pg == nil {
		return
	}

	go func() {
		var prevNodeHeight uint64
		var prevIndexedHeight uint64

		s.refreshRuntimeStatus(&prevNodeHeight, &prevIndexedHeight)

		ticker := time.NewTicker(runtimePollInterval)
		defer ticker.Stop()

		for range ticker.C {
			s.refreshRuntimeStatus(&prevNodeHeight, &prevIndexedHeight)
		}
	}()
}

func (s *Server) refreshRuntimeStatus(prevNodeHeight *uint64, prevIndexedHeight *uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodeHeight, indexedHeight, err := s.fetchRuntimeHeights(ctx)
	s.runtimeStatus.lastCheckedUnix.Store(time.Now().UnixNano())
	if err != nil {
		s.runtimeStatus.synced.Store(false)
		log.Warn().Err(err).Msg("runtime monitor: failed to refresh sync state")
		return
	}

	s.runtimeStatus.nodeHeight.Store(nodeHeight)
	s.runtimeStatus.indexedHeight.Store(indexedHeight)
	s.runtimeStatus.synced.Store(runtimeHealthOK(nodeHeight, indexedHeight, time.Now(), time.Now()))

	if prevNodeHeight != nil && prevIndexedHeight != nil {
		if (*prevNodeHeight != 0 && nodeHeight != *prevNodeHeight) || (*prevIndexedHeight != 0 && indexedHeight != *prevIndexedHeight) {
			s.archivalCache.PurgeLatest()
		}
		*prevNodeHeight = nodeHeight
		*prevIndexedHeight = indexedHeight
	}
}

func (s *Server) fetchRuntimeHeights(ctx context.Context) (uint64, uint64, error) {
	status, err := s.rpc.Status(ctx)
	if err != nil {
		return 0, 0, err
	}

	indexedHeight, err := s.pg.GetMaxHeight(ctx)
	if err != nil {
		return 0, 0, err
	}

	return uint64(status.SyncInfo.LatestBlockHeight), uint64(indexedHeight), nil
}

func (s *Server) currentRuntimeSync() (bool, uint64, uint64) {
	if s == nil || s.runtimeStatus == nil {
		return false, 0, 0
	}
	lastChecked := time.Unix(0, s.runtimeStatus.lastCheckedUnix.Load())
	nodeHeight := s.runtimeStatus.nodeHeight.Load()
	indexedHeight := s.runtimeStatus.indexedHeight.Load()
	return runtimeHealthOK(nodeHeight, indexedHeight, lastChecked, time.Now()), nodeHeight, indexedHeight
}

func runtimeHealthOK(nodeHeight uint64, indexedHeight uint64, lastChecked time.Time, now time.Time) bool {
	if nodeHeight == 0 {
		return false
	}
	if lastChecked.IsZero() || now.Sub(lastChecked) > runtimeHealthStaleAfter {
		return false
	}
	return indexedHeight+runtimeSyncLagTolerance >= nodeHeight
}
