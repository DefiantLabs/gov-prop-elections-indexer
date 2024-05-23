package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/DefiantLabs/probe/client"
	"github.com/cosmos/cosmos-sdk/x/staking/types"
)

type syncGroupResult struct {
	delegatorID          uint
	delegatorDelegations types.DelegationResponses
	delegatorError       error
}

func GetDelegationsAndMetadataAsync(
	addresses []database.OldestDelegators,
	height *int64,
	perJobLimit int,
	afterJobThrottle int,
	failureChunkLimit int,
	client *client.ChainClient,
) (map[uint]types.DelegationResponses, error) {
	// Chunk the addresses based on the per job limit
	chunks := chunkDelegators(addresses, perJobLimit)
	config.Log.Infof("Processing %d delegators in %d chunks", len(addresses), len(chunks))

	delegatorResponse := make(map[uint]types.DelegationResponses)

	allFailures := 0
	for i, chunk := range chunks {
		// Create a wait group for the chunk
		var wg sync.WaitGroup
		wg.Add(len(chunk))

		// Create a channel to store the results
		results := make(chan syncGroupResult, len(chunk))
		// Process each delegator in the chunk
		for _, delegator := range chunk {
			go func(delegator database.OldestDelegators) {
				defer wg.Done()
				config.Log.Infof("Processing delegator %s", delegator.Address)
				delegations, err := requests.GetDelegations(delegator.Address, height, *client)
				results <- syncGroupResult{
					delegatorID:          delegator.AddressID,
					delegatorDelegations: delegations,
					delegatorError:       err,
				}
			}(delegator)
		}

		// Wait for all the delegators in the chunk to finish
		wg.Wait()
		close(results)
		config.Log.Infof("Finished processing chunk")

		// Throttle the requests
		if i < len(chunks)-1 {

			for result := range results {
				if result.delegatorError != nil {
					config.Log.Errorf("Error processing delegator %d: %s", result.delegatorID, result.delegatorError)
					allFailures++
				} else {
					config.Log.Infof("Processed delegator %d", result.delegatorID)
					delegatorResponse[result.delegatorID] = result.delegatorDelegations
				}
			}

			if failureChunkLimit > 0 && allFailures >= failureChunkLimit {
				return delegatorResponse, fmt.Errorf("too many errors, stopping processing after chunk %d", i)
			}

			time.Sleep(time.Duration(afterJobThrottle) * time.Millisecond)
		}
	}

	return delegatorResponse, nil
}

func chunkDelegators(slice []database.OldestDelegators, chunkSize int) [][]database.OldestDelegators {
	var chunks [][]database.OldestDelegators
	for {
		if len(slice) == 0 {
			break
		}

		// necessary check to avoid slicing beyond
		// slice capacity
		if len(slice) < chunkSize {
			chunkSize = len(slice)
		}

		chunks = append(chunks, slice[0:chunkSize])
		slice = slice[chunkSize:]
	}

	return chunks
}
