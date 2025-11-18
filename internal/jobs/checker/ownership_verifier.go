package checker

import (
	"context"
	"fmt"
	"time"

	"magpie/internal/database"

	"github.com/charmbracelet/log"
)

const (
	ownershipBatchWindow   = 50 * time.Millisecond
	ownershipBatchMaxItems = 8192
	ownershipQueryTimeout  = 5 * time.Second
)

type ownershipRequest struct {
	proxyID uint64
	ctx     context.Context
	resp    chan ownershipResult
}

type ownershipResult struct {
	owned bool
	err   error
}

type ownershipVerifier struct {
	requests chan *ownershipRequest
}

var proxyOwnershipVerifier = newOwnershipVerifier()

func init() {
	go proxyOwnershipVerifier.run()
}

func newOwnershipVerifier() *ownershipVerifier {
	return &ownershipVerifier{
		requests: make(chan *ownershipRequest, 1024),
	}
}

func verifyProxyOwnership(ctx context.Context, proxyID uint64) (bool, error) {
	if proxyID == 0 {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	req := &ownershipRequest{
		proxyID: proxyID,
		ctx:     ctx,
		resp:    make(chan ownershipResult, 1),
	}

	select {
	case proxyOwnershipVerifier.requests <- req:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	select {
	case res := <-req.resp:
		return res.owned, res.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (v *ownershipVerifier) run() {
	batch := make([]*ownershipRequest, 0, ownershipBatchMaxItems)
	var timer *time.Timer
	var timerC <-chan time.Time

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toProcess := make([]*ownershipRequest, len(batch))
		copy(toProcess, batch)
		batch = batch[:0]
		v.processBatch(toProcess)
	}

	for {
		select {
		case req := <-v.requests:
			batch = append(batch, req)
			if len(batch) >= ownershipBatchMaxItems {
				if timer != nil {
					if !timer.Stop() {
						<-timer.C
					}
					timer = nil
					timerC = nil
				}
				flush()
				continue
			}
			if timer == nil {
				timer = time.NewTimer(ownershipBatchWindow)
				timerC = timer.C
			}
		case <-timerC:
			flush()
			timer = nil
			timerC = nil
		}
	}
}

func (v *ownershipVerifier) processBatch(batch []*ownershipRequest) {
	active := make([]*ownershipRequest, 0, len(batch))
	for _, req := range batch {
		if req == nil {
			continue
		}
		if req.ctx == nil {
			req.ctx = context.Background()
		}
		select {
		case <-req.ctx.Done():
			req.respond(false, req.ctx.Err())
		default:
			active = append(active, req)
		}
	}

	if len(active) == 0 {
		return
	}

	uniqueIDs := make([]uint64, 0, len(active))
	seen := make(map[uint64]struct{}, len(active))
	for _, req := range active {
		if _, ok := seen[req.proxyID]; ok {
			continue
		}
		seen[req.proxyID] = struct{}{}
		uniqueIDs = append(uniqueIDs, req.proxyID)
	}

	results, err := fetchProxyOwnership(uniqueIDs)
	if err != nil {
		log.Error("batch proxy ownership lookup", "error", err)
		for _, req := range active {
			req.respond(false, err)
		}
		return
	}

	for _, req := range active {
		owned := results[req.proxyID]
		req.respond(owned, nil)
	}
}

func fetchProxyOwnership(proxyIDs []uint64) (map[uint64]bool, error) {
	if len(proxyIDs) == 0 {
		return nil, nil
	}
	if database.DB == nil {
		return nil, fmt.Errorf("database not initialised")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ownershipQueryTimeout)
	defer cancel()

	var rows []uint64
	if err := database.DB.WithContext(ctx).
		Table("user_proxies").
		Distinct("proxy_id").
		Where("proxy_id IN ?", proxyIDs).
		Pluck("proxy_id", &rows).Error; err != nil {
		return nil, err
	}

	results := make(map[uint64]bool, len(proxyIDs))
	for _, id := range proxyIDs {
		results[id] = false
	}
	for _, id := range rows {
		results[id] = true
	}
	return results, nil
}

func (req *ownershipRequest) respond(owned bool, err error) {
	select {
	case req.resp <- ownershipResult{owned: owned, err: err}:
	case <-req.ctx.Done():
	default:
	}
}
