package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/DefiantLabs/cosmos-indexer/cmd"
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/cosmos-indexer/filter"
	"github.com/DefiantLabs/cosmos-indexer/indexer"
	"github.com/DefiantLabs/gov-prop-elections-indexer/appcmd"
	appcmdtask "github.com/DefiantLabs/gov-prop-elections-indexer/appcmd/task"
	"github.com/DefiantLabs/gov-prop-elections-indexer/appconfig"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/jobs"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/DefiantLabs/probe/client"
	"gorm.io/gorm"
)

const (
	MsgVoteV1Beta1 = "/cosmos.gov.v1beta1.MsgVote"
	MsgVoteV1      = "/cosmos.gov.v1.MsgVote"
	MsgAuthz       = "/cosmos.authz.v1beta1.MsgExec"
)

func generatePostIndexCustomMessagesFunction(blockIndexedChan chan models.Block) func(postData *indexer.PostIndexCustomMessageDataset) error {

	return func(postData *indexer.PostIndexCustomMessageDataset) error {
		var voters = make(map[uint]models.Address)
		for _, dataset := range *postData.IndexedDataset {
			for _, messageWrapper := range dataset.Messages {
				if messageWrapper.Message.MessageType.MessageType == MsgVoteV1Beta1 || messageWrapper.Message.MessageType.MessageType == MsgVoteV1 || messageWrapper.Message.MessageType.MessageType == MsgAuthz {

					for _, customParser := range messageWrapper.MessageParsedDatasets {
						parsedData := customParser.Data

						if parsedData == nil {
							continue
						}

						vote, okVote := (*parsedData).(database.Vote)
						votes, okVotes := (*parsedData).([]database.Vote)

						if !okVote && !okVotes {
							log.Printf("Failed to cast vote. Data: %v", customParser.Data)
							continue
						}

						if okVote {
							voters[vote.AddressID] = vote.Address
						} else if okVotes {
							for _, v := range votes {
								voters[v.AddressID] = v.Address
							}
						}
					}
				}
			}
		}

		if len(voters) == 0 {
			blockIndexedChan <- postData.IndexedBlock
			return nil
		}

		var addressIDs []uint
		for _, v := range voters {
			addressIDs = append(addressIDs, v.ID)
		}

		// Search for delegations for the addresses
		delegations := []uint{}

		err := postData.DB.
			Model(&database.VoterDelegationSnapshot{}).
			Where("address_id IN (?)", addressIDs).
			Distinct().
			Pluck("address_id", &delegations).Error

		if err != nil {
			config.Log.Errorf("Failed to find delegations. Err: %v", err)
			return err
		}

		var updatesRequired []jobs.PriorityAddressQueueData
		if len(delegations) == 0 {
			for _, v := range addressIDs {
				updatesRequired = append(updatesRequired, jobs.PriorityAddressQueueData{
					AddressID: v,
					Block:     postData.IndexedBlock,
				})
			}
		} else if len(delegations) == len(addressIDs) {
			blockIndexedChan <- postData.IndexedBlock
			return nil
		} else {
			for _, v := range delegations {
				if _, ok := voters[v]; !ok {
					updatesRequired = append(updatesRequired, jobs.PriorityAddressQueueData{
						AddressID: v,
						Block:     postData.IndexedBlock,
					})
				}
			}
		}

		if len(updatesRequired) != 0 {
			config.Log.Infof("Postprocessing found new addresses, adding %d addresses to priority queue", len(updatesRequired))
			select {
			case PriorityAddressesQueue <- updatesRequired:
			default:
				config.Log.Errorf("Failed to add addresses to priority queue. Queue is full.")
				return fmt.Errorf("failed to add addresses to priority queue. Queue is full")
			}
		}

		blockIndexedChan <- postData.IndexedBlock

		return nil
	}
}

var PriorityAddressesQueue = make(chan []jobs.PriorityAddressQueueData, 10000)

func main() {
	// Since we have extended the indexer command to include a more complex post processing workflow, we need to check the command
	// arg early and run a different workflow if the command is "index".
	if len(os.Args) < 2 {
		helpOverideExit()
	}

	cmdArg := os.Args[1]

	proposalsConfig := appconfig.ProposalsConfig{}
	blockWatcherConfig := appconfig.BlockWatcher{}
	lcdConfig := appconfig.LCDConfig{}

	SetupRootCmd(&proposalsConfig, &blockWatcherConfig, &lcdConfig)

	// Register the custom database models. They will be migrated and included in the database when the indexer finishes setup.
	customModels := database.GetCustomModels()

	indexer := cmd.GetBuiltinIndexer()

	// Register the custom types that will modify the behavior of the indexer
	indexer.RegisterCustomModels(customModels)

	switch cmdArg {
	case "index":
		customIndexWorkflow(indexer, &proposalsConfig, &blockWatcherConfig, &lcdConfig)
	default:
		err := cmd.Execute()
		if err != nil {
			config.Log.Errorf("Failed to execute. Err: %v", err)
		}
	}
}

// Due to the way the root command handles running the commands and the way the indexer is setup, we need to create a custom workflow
// Otherwise, other commands would receive setups only related to the custom indexer.
func customIndexWorkflow(indexer *indexer.Indexer, proposalsConfig *appconfig.ProposalsConfig, blockWatcherConfig *appconfig.BlockWatcher, lcdConfig *appconfig.LCDConfig) {
	// This indexer is only concerned with vote messages, so we create regex filters to only index those messages.
	// This significantly reduces the size of the indexed dataset, saving space and processing time.
	// We use a regex because the message type can be different between v1 and v1beta1 of the gov module.
	govVoteRegexMessageTypeFilter, err := filter.NewRegexMessageTypeFilter("^/cosmos\\.gov.*MsgVote$")
	if err != nil {
		log.Fatalf("Failed to create regex message type filter. Err: %v", err)
	}
	authzRegexMessageTypeFilter, err := filter.NewRegexMessageTypeFilter("^/cosmos\\.authz\\.v1beta1\\.MsgExec$")
	if err != nil {
		log.Fatalf("Failed to create regex message type filter. Err: %v", err)
	}

	blockIndexedChan := make(chan models.Block, 1000)

	indexer.RegisterMessageTypeFilter(govVoteRegexMessageTypeFilter)
	indexer.RegisterMessageTypeFilter(authzRegexMessageTypeFilter)
	indexer.PostIndexCustomMessageFunction = generatePostIndexCustomMessagesFunction(blockIndexedChan)
	indexer.PostSetupCustomFunction = postSetupCustomFunction
	indexer.PreExitCustomFunction = generatePreExitCustomFunction(blockIndexedChan)

	// Register the custom message parser for the vote message types. Our parser can handle both v1 and v1beta1 vote messages.
	// However, they must be uniquely identified by the Identifier() method. This will make identifying any parser errors easier.
	v1Beta1VoteParser := &MsgVoteParser{Id: "vote-v1beta1", ProposalsConfig: proposalsConfig}
	v1VoteParser := &MsgVoteParser{Id: "vote-v1", ProposalsConfig: proposalsConfig}
	authzExecParser := &MsgAuthzExec{Id: "authz-exec", ProposalsConfig: proposalsConfig}
	indexer.RegisterCustomMessageParser(MsgVoteV1Beta1, v1Beta1VoteParser)
	indexer.RegisterCustomMessageParser(MsgVoteV1, v1VoteParser)
	indexer.RegisterCustomMessageParser(MsgAuthz, authzExecParser)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	// Execute the root command to start the indexer.
	go func() {
		defer wg.Done()
		err = cmd.Execute()
		if err != nil {
			config.Log.Fatalf("Failed to execute. Err: %v", err)
		}
		config.Log.Infof("Closing queue")
		close(PriorityAddressesQueue)
	}()

	indexerSetup := <-indexer.PostSetupDatasetChannel

	if indexerSetup != nil {
		config.Log.Infof("Indexer setup finished, using configs from custom indexer workflow to start downstream processors")

		probeClient := indexerSetup.ChainClient
		db, err := cmd.ConnectToDBAndMigrate(indexerSetup.Config.Database)

		if err != nil {
			config.Log.Fatalf("Failed to connect to database and run migrations. Err: %v", err)
		}

		var proposalIDs []uint64
		for _, proposalID := range proposalsConfig.ProposalAllowList {
			proposalIDs = append(proposalIDs, uint64(proposalID))
		}

		if proposalsConfig.ClydeProposal != 0 {
			proposalIDs = append(proposalIDs, uint64(proposalsConfig.ClydeProposal))
		}

		if proposalsConfig.GraceProposal != 0 {
			proposalIDs = append(proposalIDs, uint64(proposalsConfig.GraceProposal))
		}

		if proposalsConfig.MattProposal != 0 {
			proposalIDs = append(proposalIDs, uint64(proposalsConfig.MattProposal))
		}

		var proposals []database.Proposal
		err = db.Table("proposals").Select("id, proposal_id").Find(&proposals).Error

		if err != nil {
			config.Log.Fatalf("Failed to get proposals in DB. Err: %v", err)
		}

		for _, proposal := range proposals {
			proposalIDs = append(proposalIDs, proposal.ProposalID)
		}

		onchainProposals, err := getOnChainProposals(proposalIDs, lcdConfig.URL)

		if err != nil {
			config.Log.Fatalf("Failed to get on chain proposals. Err: %v", err)
		}

		onchainProposalsToVotingEndTimes := make(map[uint64]time.Time)
		for proposalID, proposal := range onchainProposals {
			onchainProposalsToVotingEndTimes[proposalID] = proposal.VotingEndTime
		}

		wg.Add(1)
		go jobs.ProcessPriorityAddressesForDelegations(PriorityAddressesQueue, wg, db, indexer.Config, indexerSetup.ChainClient)

		wg.Add(1)

		go blockWatcher(wg, blockIndexedChan, probeClient, db, indexerSetup.Config.Probe, blockWatcherConfig, onchainProposalsToVotingEndTimes, lcdConfig.URL)

		wg.Wait()

		config.Log.Infof("Indexer finished, exiting")
	}
}

func getOnChainProposals(proposalIDs []uint64, lcdURL string) (map[uint64]*requests.OnChainProposal, error) {
	var onChainProposals = make(map[uint64]*requests.OnChainProposal)

	for _, proposalID := range proposalIDs {

		if _, ok := onChainProposals[proposalID]; ok {
			continue
		}

		onChainProposal, err := requests.GetOnChainProposalByID(proposalID, lcdURL)
		if err != nil {
			config.Log.Errorf("Failed to get on chain proposal for proposal %d. Err: %v", proposalID, err)
			return nil, err
		}

		onChainProposals[proposalID] = onChainProposal
	}

	return onChainProposals, nil
}

func blockWatcher(parentWg *sync.WaitGroup, blockIndexedChan chan models.Block, probeClient *client.ChainClient, db *gorm.DB, probeConfig config.Probe, blockWatcherConfig *appconfig.BlockWatcher, onchainProposalsToVotingEndTimeMap map[uint64]time.Time, lcdURL string) {
	defer parentWg.Done()

	config.Log.Infof("Reading from blockIndexedChan")

	blockWatcherWg := &sync.WaitGroup{}
	for block := range blockIndexedChan {
		config.Log.Infof("Received block finished signal for block %d", block.Height)
		inBlock := block
		if block.Height%blockWatcherConfig.StakingUpdateBlockThreshold == 0 {
			config.Log.Infof("Found block %d at threshold, running snapshot updater", block.Height)

			blockWatcherWg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				jobs.UpdateDelegatorAndValidatorPowerAtBlock(
					&blockWatcherConfig.StakingUpdateDelegatorLimit,
					blockWatcherConfig.StakingUpdateConcurrentRequests,
					blockWatcherConfig.StakingUpdateBlockThrottle,
					blockWatcherConfig.StakingUpdateErrorLimit,
					&inBlock,
					db,
					probeClient,
					&probeConfig,
				)

			}(blockWatcherWg)
		}

		if block.Height%blockWatcherConfig.ProposalStatusUpdateThreshold == 0 {
			config.Log.Infof("Found block %d at proposal status threshold, running proposal status updater", block.Height)

			blockWatcherWg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				jobs.UpdateProposalStatusAtBlock(
					blockWatcherConfig.ProposalStatusUpdateInterval,
					&inBlock,
					onchainProposalsToVotingEndTimeMap,
					db,
					lcdURL,
				)
			}(blockWatcherWg)
		}
	}

	blockWatcherWg.Wait()
	config.Log.Infof("Exiting block watcher")
}

func generatePreExitCustomFunction(blockIndexedChan chan models.Block) func(*indexer.PreExitCustomDataset) error {
	// we are acting as the writer here, closing the channel when the indexer says it is finished
	return func(dataset *indexer.PreExitCustomDataset) error {
		config.Log.Infof("Indexer finished, running custom pre-exit function")
		close(blockIndexedChan)
		return nil
	}
}

func postSetupCustomFunction(indexerPostSetupData indexer.PostSetupCustomDataset) error {
	config.Log.Infof("Indexer setup finished, using configs from custom indexer workflow to do custom setup")
	// Stubbed as example
	return nil
}

func helpOverideExit() {
	SetupRootCmd(&appconfig.ProposalsConfig{}, &appconfig.BlockWatcher{}, &appconfig.LCDConfig{})
	rootCommand := cmd.GetRootCmd()
	err := rootCommand.Help()
	if err != nil {
		config.Log.Fatal("Please provide a command line argument for the command to run")
	}
	os.Exit(1)
}

func SetupRootCmd(proposalsConfig *appconfig.ProposalsConfig, blockWatcherConfig *appconfig.BlockWatcher, lcdConfig *appconfig.LCDConfig) {
	rootCommand := cmd.GetRootCmd()
	rootCommand.Use = "gov-prop-elections-indexer"

	rootCommand.AddCommand(appcmd.APICmd)
	rootCommand.AddCommand(appcmdtask.TaskCmd)

	rootCommand.PersistentFlags().UintSliceVar(&proposalsConfig.ProposalAllowList, "proposals.proposal-allow-list", []uint{}, "proposals to index")
	rootCommand.PersistentFlags().UintVar(&proposalsConfig.ClydeProposal, "proposals.clyde-proposal", 0, "proposal for clyde")
	rootCommand.PersistentFlags().UintVar(&proposalsConfig.GraceProposal, "proposals.grace-proposal", 0, "proposal for grace")
	rootCommand.PersistentFlags().UintVar(&proposalsConfig.MattProposal, "proposals.matt-proposal", 0, "proposal for matt")
	appconfig.SetupBlockWatcherFlags(blockWatcherConfig, rootCommand)
	appconfig.SetupLCDFlags(lcdConfig, rootCommand)
}
