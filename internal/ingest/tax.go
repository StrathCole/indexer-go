package ingest

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"

	wasmtypes "github.com/CosmWasm/wasmd/x/wasm/types"
	markettypes "github.com/classic-terra/core/v3/x/market/types"
	treasurytypes "github.com/classic-terra/core/v3/x/treasury/types"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	grpctypes "github.com/cosmos/cosmos-sdk/types/grpc"
	authz "github.com/cosmos/cosmos-sdk/x/authz"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	BondDenom            = "uluna"
	BurnTaxUpgradeHeight = 9346889
	BurnTaxReworkHeight  = 21163600
	BlocksPerWeek        = 100800
)

type TaxPolicy struct {
	Rate          sdk.Dec
	Caps          map[string]sdk.Int
	ExemptionList []string
	PolicyCap     sdk.Int
}

// CachedTaxPolicy includes validity range
type CachedTaxPolicy struct {
	Policy    *TaxPolicy
	FromBlock int64 // First block this policy is valid for
	ToBlock   int64 // Last block this policy is valid for (0 = unknown/ongoing)
}

type TaxCalculator struct {
	grpcConn *grpc.ClientConn

	// Cache of policies with their validity ranges
	// Key is the starting block of the policy
	policyCache map[int64]*CachedTaxPolicy
	mu          sync.RWMutex

	// Track the highest block we've verified policy for
	verifiedUpTo int64

	// Maximum cache size to prevent unbounded growth
	maxCacheSize int
}

func NewTaxCalculator(conn *grpc.ClientConn) *TaxCalculator {
	return &TaxCalculator{
		grpcConn:     conn,
		policyCache:  make(map[int64]*CachedTaxPolicy),
		maxCacheSize: 100, // Keep at most 100 policy entries
	}
}

// fetchPolicyAtHeight fetches the tax policy at a specific height via gRPC
func (tc *TaxCalculator) fetchPolicyAtHeight(ctx context.Context, height int64) (*TaxPolicy, error) {
	if tc.grpcConn == nil {
		return nil, fmt.Errorf("grpc connection is nil")
	}

	md := metadata.Pairs(grpctypes.GRPCBlockHeightHeader, strconv.FormatInt(height, 10))
	ctx = metadata.NewOutgoingContext(ctx, md)

	treasuryClient := treasurytypes.NewQueryClient(tc.grpcConn)

	// Fetch Tax Rate
	var rate sdk.Dec
	r, err := tc.fetchTaxRateGRPC(ctx, "/terra.treasury.v1beta1.Query/TaxRate")
	if err == nil {
		rate = r
	} else {
		if height >= BurnTaxReworkHeight {
			r, err := tc.fetchTaxRateGRPC(ctx, "/terra.tax.v1beta1.Query/BurnTaxRate")
			if err != nil {
				return nil, fmt.Errorf("failed to fetch burn tax rate: %w", err)
			}
			rate = r
		} else {
			return nil, fmt.Errorf("failed to fetch tax rate: %w", err)
		}
	}

	// Fetch Tax Caps
	capsRes, err := treasuryClient.TaxCaps(ctx, &treasurytypes.QueryTaxCapsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tax caps: %w", err)
	}
	caps := make(map[string]sdk.Int)
	for _, c := range capsRes.TaxCaps {
		caps[c.Denom] = c.TaxCap
	}

	// Fetch Params (Policy Cap)
	paramsRes, err := treasuryClient.Params(ctx, &treasurytypes.QueryParamsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch treasury params: %w", err)
	}
	policyCap := paramsRes.Params.TaxPolicy.Cap.Amount

	// Fetch Exemption List
	var exemptionList []string
	exemptionRes, err := treasuryClient.BurnTaxExemptionList(ctx, &treasurytypes.QueryBurnTaxExemptionListRequest{})
	if err == nil {
		exemptionList = exemptionRes.Addresses
	}

	return &TaxPolicy{
		Rate:          rate,
		Caps:          caps,
		ExemptionList: exemptionList,
		PolicyCap:     policyCap,
	}, nil
}

// policiesEqual compares two tax policies for equality
func (tc *TaxCalculator) policiesEqual(a, b *TaxPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}

	// Compare rate
	if !a.Rate.Equal(b.Rate) {
		return false
	}

	// Compare policy cap
	if !a.PolicyCap.Equal(b.PolicyCap) {
		return false
	}

	// Compare caps
	if len(a.Caps) != len(b.Caps) {
		return false
	}
	for denom, capA := range a.Caps {
		capB, ok := b.Caps[denom]
		if !ok || !capA.Equal(capB) {
			return false
		}
	}

	// Compare exemption list
	if len(a.ExemptionList) != len(b.ExemptionList) {
		return false
	}
	exemptionMapA := make(map[string]bool)
	for _, addr := range a.ExemptionList {
		exemptionMapA[addr] = true
	}
	for _, addr := range b.ExemptionList {
		if !exemptionMapA[addr] {
			return false
		}
	}

	return true
}

// findPolicyChangeBlock uses binary search to find the exact block where policy changed
func (tc *TaxCalculator) findPolicyChangeBlock(ctx context.Context, startBlock, endBlock int64, startPolicy *TaxPolicy) (int64, error) {
	// Binary search between startBlock and endBlock
	low := startBlock
	high := endBlock

	for low < high {
		mid := (low + high) / 2

		midPolicy, err := tc.fetchPolicyAtHeight(ctx, mid)
		if err != nil {
			return 0, err
		}

		if tc.policiesEqual(startPolicy, midPolicy) {
			// Policy is still the same at mid, change is after mid
			low = mid + 1
		} else {
			// Policy changed at or before mid
			high = mid
		}
	}

	return low, nil
}

// GetTaxPolicy returns the tax policy for a given height, using cached ranges
func (tc *TaxCalculator) GetTaxPolicy(ctx context.Context, height int64) (*TaxPolicy, error) {
	tc.mu.RLock()
	// Find cached policy that covers this height
	for startBlock, cached := range tc.policyCache {
		if height >= startBlock && (cached.ToBlock == 0 || height <= cached.ToBlock) {
			tc.mu.RUnlock()
			return cached.Policy, nil
		}
	}
	tc.mu.RUnlock()

	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Double-check after acquiring write lock
	for startBlock, cached := range tc.policyCache {
		if height >= startBlock && (cached.ToBlock == 0 || height <= cached.ToBlock) {
			return cached.Policy, nil
		}
	}

	// Fetch policy at this height
	policy, err := tc.fetchPolicyAtHeight(ctx, height)
	if err != nil {
		return nil, err
	}

	// Look ahead to see if policy changes soon
	// Check one epoch ahead
	lookAheadBlock := height + BlocksPerWeek
	lookAheadPolicy, err := tc.fetchPolicyAtHeight(ctx, lookAheadBlock)

	var toBlock int64 = 0 // Unknown end

	if err == nil {
		if tc.policiesEqual(policy, lookAheadPolicy) {
			// Policy is the same for at least one epoch ahead
			// We can safely cache for this range
			toBlock = 0 // Will be updated when we find a change
		} else {
			// Policy changed within this epoch - find exact change point
			changeBlock, err := tc.findPolicyChangeBlock(ctx, height, lookAheadBlock, policy)
			if err != nil {
				log.Printf("Tax: Failed to find policy change block: %v", err)
				// Still cache current policy but with conservative range
				toBlock = height + BlocksPerWeek/10 // Cache for ~1/10 of an epoch
			} else {
				toBlock = changeBlock - 1
				log.Printf("Tax: Policy changes at block %d", changeBlock)
			}
		}
	}

	// Prune cache if it exceeds max size (keep most recent entries)
	if len(tc.policyCache) >= tc.maxCacheSize {
		tc.pruneCache()
	}

	// Store in cache
	tc.policyCache[height] = &CachedTaxPolicy{
		Policy:    policy,
		FromBlock: height,
		ToBlock:   toBlock,
	}

	// If we determined a valid end block, also prefetch and cache the next policy
	if toBlock > 0 && err == nil {
		nextPolicy, err := tc.fetchPolicyAtHeight(ctx, toBlock+1)
		if err == nil {
			tc.policyCache[toBlock+1] = &CachedTaxPolicy{
				Policy:    nextPolicy,
				FromBlock: toBlock + 1,
				ToBlock:   0,
			}
		}
	}

	return policy, nil
}

// pruneCache removes the oldest entries to keep cache size bounded
// Must be called with write lock held
func (tc *TaxCalculator) pruneCache() {
	if len(tc.policyCache) < tc.maxCacheSize/2 {
		return
	}

	// Find entries with defined ToBlock (completed ranges) - these are safe to remove
	// Keep entries where ToBlock == 0 (ongoing/unknown end) as they might still be needed
	var keysToRemove []int64
	for k, v := range tc.policyCache {
		if v.ToBlock > 0 {
			keysToRemove = append(keysToRemove, k)
		}
	}

	// Remove half of the completed entries (oldest first by FromBlock)
	removeCount := len(keysToRemove) / 2
	if removeCount == 0 && len(keysToRemove) > 0 {
		removeCount = 1
	}

	// Simple removal of first entries (not perfectly sorted, but good enough)
	for i := 0; i < removeCount && i < len(keysToRemove); i++ {
		delete(tc.policyCache, keysToRemove[i])
	}
}

func (tc *TaxCalculator) CalculateTax(ctx context.Context, height int64, tx sdk.Tx, cdc codec.Codec) (sdk.Coins, error) {
	policy, err := tc.GetTaxPolicy(ctx, height)
	if err != nil {
		return nil, err
	}

	var totalTax sdk.Coins

	for _, msg := range tx.GetMsgs() {
		taxCoins := tc.getTaxCoins(msg, height, policy, cdc)

		for _, coin := range taxCoins {
			// Columbus-5 no tax for Luna until burn tax upgrade
			if coin.Denom == BondDenom && height < BurnTaxUpgradeHeight {
				continue
			}

			cap, ok := policy.Caps[coin.Denom]
			if !ok {
				cap = policy.PolicyCap
			}

			// tax = min(amount * rate, cap)
			taxAmount := sdk.NewDecFromInt(coin.Amount).Mul(policy.Rate).TruncateInt()
			if taxAmount.GT(cap) {
				taxAmount = cap
			}

			if taxAmount.IsPositive() {
				totalTax = totalTax.Add(sdk.NewCoin(coin.Denom, taxAmount))
			}
		}
	}

	return totalTax, nil
}

func (tc *TaxCalculator) getTaxCoins(msg sdk.Msg, height int64, policy *TaxPolicy, cdc codec.Codec) []sdk.Coin {
	var coins []sdk.Coin

	switch m := msg.(type) {
	case *banktypes.MsgSend:
		if tc.isExemptionAddress(m.FromAddress, m.ToAddress, policy) {
			break
		}
		coins = m.Amount

	case *banktypes.MsgMultiSend:
		for i, input := range m.Inputs {
			if i < len(m.Outputs) {
				output := m.Outputs[i]
				if !tc.isExemptionAddress(input.Address, output.Address, policy) {
					coins = append(coins, input.Coins...)
				}
			}
		}

	case *markettypes.MsgSwapSend:
		coins = []sdk.Coin{m.OfferCoin}

	case *wasmtypes.MsgInstantiateContract:
		if height < BurnTaxReworkHeight {
			coins = m.Funds
		}

	case *wasmtypes.MsgInstantiateContract2:
		if height < BurnTaxReworkHeight {
			coins = m.Funds
		}

	case *wasmtypes.MsgExecuteContract:
		if height < BurnTaxReworkHeight {
			coins = m.Funds
		}

	case *authz.MsgExec:
		for _, innerMsg := range m.Msgs {
			var unpacked sdk.Msg
			err := cdc.UnpackAny(innerMsg, &unpacked)
			if err == nil {
				coins = append(coins, tc.getTaxCoins(unpacked, height, policy, cdc)...)
			}
		}
	}

	return coins
}

func (tc *TaxCalculator) isExemptionAddress(from, to string, policy *TaxPolicy) bool {
	isFrom := false
	isTo := false
	for _, addr := range policy.ExemptionList {
		if addr == from {
			isFrom = true
		}
		if addr == to {
			isTo = true
		}
	}
	return isFrom && isTo
}

type RawTaxRateResponse struct {
	TaxRate string `protobuf:"bytes,1,opt,name=tax_rate,json=taxRate,proto3" json:"tax_rate,omitempty"`
}

func (m *RawTaxRateResponse) Reset()         { *m = RawTaxRateResponse{} }
func (m *RawTaxRateResponse) String() string { return fmt.Sprintf("%v", *m) }
func (m *RawTaxRateResponse) ProtoMessage()  {}

func (tc *TaxCalculator) fetchTaxRateGRPC(ctx context.Context, method string) (sdk.Dec, error) {
	out := new(RawTaxRateResponse)
	err := tc.grpcConn.Invoke(ctx, method, nil, out)
	if err != nil {
		return sdk.Dec{}, err
	}

	dec, err := sdk.NewDecFromStr(out.TaxRate)
	if err != nil {
		return sdk.Dec{}, err
	}

	// Heuristic: If tax rate is > 1, it's likely an unscaled integer (10^18)
	if dec.GT(sdk.OneDec()) {
		precision := sdk.NewDecFromIntWithPrec(sdk.OneInt(), 18)
		dec = dec.Mul(precision)
	}

	return dec, nil
}
