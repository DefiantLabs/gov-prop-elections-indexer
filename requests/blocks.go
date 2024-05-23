package requests

import (
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/cosmos-indexer/rpc"
	"github.com/DefiantLabs/probe/client"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
)

func GetLatestCommittedHeight(chainClient *client.ChainClient) (*coretypes.ResultBlock, error) {
	// Get the latest block height
	latestHeight, err := rpc.GetLatestBlockHeightWithRetry(chainClient, 5, 5)
	if err != nil {
		config.Log.Error("Failed to get latest block height", err)
		return nil, err
	}

	// Get committed height, not height from status
	latestHeight = latestHeight - 1

	// Might need the timestamp downstream
	blockRPC, err := rpc.GetBlock(chainClient, latestHeight)

	if err != nil {
		config.Log.Errorf("Failed to get block at height %d. Err: %v", latestHeight, err)
		return nil, err
	}

	return blockRPC, nil

}
