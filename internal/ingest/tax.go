package ingest

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	wasmtypes "github.com/CosmWasm/wasmd/x/wasm/types"
	markettypes "github.com/classic-terra/core/v3/x/market/types"

	// taxtypes "github.com/classic-terra/core/v3/x/tax/types"
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

type CachedPolicy struct {
	Rate sdk.Dec
	Caps map[string]sdk.Int
}

type BlockPolicy struct {
	ExemptionList []string
	PolicyCap     sdk.Int
	Height        int64 // Height at which this was fetched
}

type TaxCalculator struct {
	grpcConn *grpc.ClientConn
	cache    map[int64]*CachedPolicy
	mu       sync.RWMutex

	blockCache  *BlockPolicy
	blockMu     sync.RWMutex
}

func NewTaxCalculator(conn *grpc.ClientConn) *TaxCalculator {
	return &TaxCalculator{
		grpcConn: conn,
		cache:    make(map[int64]*CachedPolicy),
	}
}

func (tc *TaxCalculator) GetTaxPolicy(ctx context.Context, height int64) (*TaxPolicy, error) {
	if tc.grpcConn == nil {
		return nil, fmt.Errorf("grpc connection is nil")
	}

	// Prepare context with height
	md := metadata.Pairs(grpctypes.GRPCBlockHeightHeader, strconv.FormatInt(height, 10))
	ctx = metadata.NewOutgoingContext(ctx, md)

	treasuryClient := treasurytypes.NewQueryClient(tc.grpcConn)

	// Cache Block Policy (ExemptionList, PolicyCap) per epoch - these don't change often
	// Use epoch-based caching (same as rate/caps) to avoid per-block gRPC calls
	epoch := height / BlocksPerWeek
	
	tc.blockMu.RLock()
	blockPolicy := tc.blockCache
	needBlockFetch := blockPolicy == nil || (blockPolicy.Height/BlocksPerWeek) != epoch
	tc.blockMu.RUnlock()

	if needBlockFetch {
		tc.blockMu.Lock()
		// Double-check after acquiring write lock
		if tc.blockCache == nil || (tc.blockCache.Height/BlocksPerWeek) != epoch {
			// Fetch Params (Policy Cap)
			paramsRes, err := treasuryClient.Params(ctx, &treasurytypes.QueryParamsRequest{})
			if err != nil {
				tc.blockMu.Unlock()
				return nil, fmt.Errorf("failed to fetch treasury params: %w", err)
			}
			policyCap := paramsRes.Params.TaxPolicy.Cap.Amount

			// Fetch Exemption List
			var exemptionList []string
			exemptionRes, err := treasuryClient.BurnTaxExemptionList(ctx, &treasurytypes.QueryBurnTaxExemptionListRequest{})
			if err == nil {
				exemptionList = exemptionRes.Addresses
			}

			tc.blockCache = &BlockPolicy{
				ExemptionList: exemptionList,
				PolicyCap:     policyCap,
				Height:        height,
			}
		}
		blockPolicy = tc.blockCache
		tc.blockMu.Unlock()
	}

	// Cache Rate and Caps by epoch (week) - epoch already calculated above
	tc.mu.RLock()
	cached, ok := tc.cache[epoch]
	tc.mu.RUnlock()

	if !ok {
		tc.mu.Lock()
		// Double check
		if cached, ok = tc.cache[epoch]; !ok {
			// Fetch Tax Rate
			var rate sdk.Dec

			// Use raw gRPC to avoid proto unmarshalling issues with custom types
			r, err := tc.fetchTaxRateGRPC(ctx, "/terra.treasury.v1beta1.Query/TaxRate")
			if err == nil {
				rate = r
			} else {
				if height >= BurnTaxReworkHeight {
					r, err := tc.fetchTaxRateGRPC(ctx, "/terra.tax.v1beta1.Query/BurnTaxRate")
					if err != nil {
						tc.mu.Unlock()
						return nil, fmt.Errorf("failed to fetch burn tax rate: %w", err)
					}
					rate = r
				} else {
					tc.mu.Unlock()
					return nil, fmt.Errorf("failed to fetch tax rate: %w", err)
				}
			}

			// Fetch Tax Caps
			capsRes, err := treasuryClient.TaxCaps(ctx, &treasurytypes.QueryTaxCapsRequest{})
			if err != nil {
				tc.mu.Unlock()
				return nil, fmt.Errorf("failed to fetch tax caps: %w", err)
			}
			caps := make(map[string]sdk.Int)
			for _, c := range capsRes.TaxCaps {
				caps[c.Denom] = c.TaxCap
			}

			cached = &CachedPolicy{
				Rate: rate,
				Caps: caps,
			}
			tc.cache[epoch] = cached
		}
		tc.mu.Unlock()
	}

	return &TaxPolicy{
		Rate:          cached.Rate,
		Caps:          cached.Caps,
		ExemptionList: blockPolicy.ExemptionList,
		PolicyCap:     blockPolicy.PolicyCap,
	}, nil
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
	// log.Printf("Raw Tax Rate: %s", out.TaxRate)
	dec, err := sdk.NewDecFromStr(out.TaxRate)
	if err != nil {
		return sdk.Dec{}, err
	}

	// Heuristic: If tax rate is > 1, it's likely an unscaled integer (10^18)
	// Tax rate should be small (e.g. 0.005).
	if dec.GT(sdk.OneDec()) {
		// Divide by 10^18
		precision := sdk.NewDecFromIntWithPrec(sdk.OneInt(), 18) // 10^-18
		// Wait, NewDecFromIntWithPrec(1, 18) is 0.000...01
		// We want to multiply by 10^-18
		dec = dec.Mul(precision)
	}

	return dec, nil
}
