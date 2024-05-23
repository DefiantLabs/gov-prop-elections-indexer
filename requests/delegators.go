package requests

import (
	"github.com/DefiantLabs/probe/client"
	probeQuery "github.com/DefiantLabs/probe/query"
	probeStaking "github.com/DefiantLabs/probe/query/staking"
	cosmosQuery "github.com/cosmos/cosmos-sdk/types/query"
	stakingTypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

func GetDelegations(address string, height *int64, client client.ChainClient) (stakingTypes.DelegationResponses, error) {
	options := probeQuery.QueryOptions{}
	query := &probeQuery.Query{Client: &client, Options: &options}

	if height != nil {
		query.Options.Height = *height
	}

	var delegationResponses stakingTypes.DelegationResponses
	var nextKey []byte
	for {
		if nextKey != nil {
			query.Options.Pagination = &cosmosQuery.PageRequest{Key: nextKey}
		} else {
			query.Options.Pagination = nil
		}

		resp, err := probeStaking.DelegatorDelegations(query, address)
		if err != nil {
			return nil, err
		}

		delegationResponses = append(delegationResponses, resp.DelegationResponses...)

		if resp.Pagination == nil || resp.Pagination.NextKey == nil {
			break
		}

		nextKey = resp.Pagination.NextKey
	}

	return delegationResponses, nil
}
