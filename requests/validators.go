package requests

import (
	"github.com/DefiantLabs/probe/client"
	probeQuery "github.com/DefiantLabs/probe/query"
	probeStaking "github.com/DefiantLabs/probe/query/staking"
	cosmosQuery "github.com/cosmos/cosmos-sdk/types/query"
	stakingTypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

func GetValidatorsAtHeight(chainClient *client.ChainClient, height int64) ([]stakingTypes.Validator, error) {
	options := probeQuery.QueryOptions{
		Height: height,
	}

	query := &probeQuery.Query{Client: chainClient, Options: &options}
	bondedStatus := stakingTypes.Bonded
	paginationKey := []byte{}
	validators := []stakingTypes.Validator{}
	for {
		if paginationKey != nil {
			query.Options.Pagination = &cosmosQuery.PageRequest{
				Key: paginationKey,
			}
		}

		// We only care about bonded validators here because they are the only ones who's votes matter
		vals, err := probeStaking.Validators(query, &bondedStatus)
		if err != nil {
			return nil, err
		}

		validators = append(validators, vals.Validators...)

		if vals.Pagination.NextKey == nil {
			break
		}

		paginationKey = vals.Pagination.NextKey
	}

	return validators, nil
}
