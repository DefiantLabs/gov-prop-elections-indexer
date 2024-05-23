package jobs

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	cosmosMath "cosmossdk.io/math"
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/DefiantLabs/gov-prop-elections-indexer/utils"
	"github.com/DefiantLabs/probe/client"
	"github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These are intended to share the load of updating delegations and validator powers
const (
	AllUpdatePowerLimit               = 500
	DelegatorsOnlyUpdatePowerLimit    = 2000
	DelegatorsUpdateFailureMaxPercent = 0.5
)

func UpdateOldestNVoterDelegationsAtBlock(
	delegatorsLimit int,
	requestJobChunkLimit int,
	requestJobChunkThrottleMilliseconds int,
	requestJobChunkFailureLimit float64,
	block *models.Block,
	db *gorm.DB,
	chainClient *client.ChainClient,
	conf *config.Probe,
) ([]*database.VoterDelegation, error) {

	if delegatorsLimit > DelegatorsOnlyUpdatePowerLimit {
		return nil, fmt.Errorf("n must be less than or equal to %d", DelegatorsOnlyUpdatePowerLimit)
	}

	// Get the oldest n delegators
	delegators, err := database.GetOldestVotersForDelegations(delegatorsLimit, db)

	if err != nil {
		return nil, err
	}

	if len(delegators) == 0 {
		config.Log.Infof("No delegators to update")
		return nil, nil
	}

	// translate failure limit to an int based on the len of delegators to process
	requestJobChunkFailureLimitInt := int(float64(len(delegators)) * requestJobChunkFailureLimit)

	var delegatorDelegations = make(map[uint]types.DelegationResponses)
	var uniqueValidators = make(map[string]database.Validator)
	var delegatorUniqueValidators = make(map[uint]map[string]struct{})
	delegatorsResponses, err := utils.GetDelegationsAndMetadataAsync(
		delegators,
		&block.Height,
		requestJobChunkLimit,
		requestJobChunkThrottleMilliseconds,
		requestJobChunkFailureLimitInt,
		chainClient,
	)

	var continuedError error
	if err != nil && len(delegatorsResponses) == 0 {
		return nil, fmt.Errorf("error failures getting delegations. Err: %v", err)
	} else if err != nil {
		continuedError = err
		config.Log.Errorf("Error during async get of delegations, will continue processing. Err: %v", err)
	}

	for delegatorID, delegatorResponse := range delegatorsResponses {
		delegatorDelegations[delegatorID] = delegatorResponse
		delegatorUniqueValidators[delegatorID] = make(map[string]struct{})
		// Get unique validators for each delegator
		for _, delegation := range delegatorResponse {
			uniqueValidators[delegation.Delegation.ValidatorAddress] = database.Validator{}
			delegatorUniqueValidators[delegatorID][delegation.Delegation.ValidatorAddress] = struct{}{}
		}
	}

	var processedDelegationModels []*database.VoterDelegation
	var processedDelegationsByValidator = make(map[string][]*database.VoterDelegation)

	err = db.Transaction(func(tx *gorm.DB) error {
		for validatorAddress := range uniqueValidators {
			validatorAddressModel, err := database.TranslateAndFindOrCreateValidatorByAccountAddress(db, validatorAddress, conf.AccountPrefix)

			if err != nil {
				config.Log.Errorf("Failed to find or create validator. Err: %v", err)
				return err
			}

			uniqueValidators[validatorAddress] = validatorAddressModel
		}

		var processedDelegatorDelegations []*database.VoterDelegation

		timeNow := time.Now()

		for delegatorID, delegations := range delegatorDelegations {

			voterSnapshot := database.VoterDelegationSnapshot{
				BlockID:   block.ID,
				AddressID: delegatorID,
				UpdatedAt: timeNow,
			}

			err := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "block_id"}, {Name: "address_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"updated_at"}),
			}).Create(&voterSnapshot).Error

			if err != nil {
				return err
			}

			for _, delegation := range delegations {
				validatorAddressModel := uniqueValidators[delegation.Delegation.ValidatorAddress]

				if !delegation.Balance.Amount.IsZero() {
					delegationModel := database.VoterDelegation{
						ValidatorID:               validatorAddressModel.ID,
						Amount:                    delegation.Balance.Amount.String(),
						VoterDelegationSnapshotID: voterSnapshot.ID,
					}

					processedDelegatorDelegations = append(processedDelegatorDelegations, &delegationModel)
					processedDelegationsByValidator[delegation.Delegation.ValidatorAddress] = append(
						processedDelegationsByValidator[delegation.Delegation.ValidatorAddress], &delegationModel)
				}
			}
		}

		if len(processedDelegatorDelegations) == 0 {
			return nil
		}

		err = db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "voter_delegation_snapshot_id"}, {Name: "validator_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"amount"}),
		}).Create(&processedDelegatorDelegations).Error

		if err != nil {
			return err
		}
		processedDelegationModels = processedDelegatorDelegations
		return nil
	})

	if err != nil && continuedError != nil {
		return nil, fmt.Errorf("error during async chunk processing %v, error updating delegations for oldest addresses. Err: %v", continuedError, err)
	} else if continuedError != nil {
		config.Log.Errorf("Error during async chunk processing %v, will continue processing. Err: %v", continuedError, err)
		return processedDelegationModels, nil
	} else if err != nil {
		config.Log.Errorf("Failed to update delegations for oldest addresses. Err: %v", err)
		return nil, err
	}

	return processedDelegationModels, nil
}

func UpdateDelegatorAndValidatorPowerAtBlock(
	delegatorsLimit *int,
	requestJobChunkLimit int,
	requestJobChunkThrottleMilliseconds int,
	requestJobChunkFailureLimit float64,
	blockModel *models.Block,
	db *gorm.DB,
	chainClient *client.ChainClient,
	conf *config.Probe,
) error {
	runLimit := AllUpdatePowerLimit
	if delegatorsLimit != nil && *delegatorsLimit > AllUpdatePowerLimit {
		return fmt.Errorf("delegatorsLimit must be less than or equal to %d", AllUpdatePowerLimit)
	} else if delegatorsLimit != nil {
		runLimit = *delegatorsLimit
	} else {
		runLimit = AllUpdatePowerLimit
	}

	block, _, _, err := getAndUpdateValidatorsAtBlock(blockModel, db, chainClient, conf)
	if err != nil {
		config.Log.Errorf("Failed to get and update validators at block %d. Err: %v", blockModel.Height, err)
		return err
	}

	_, err = UpdateOldestNVoterDelegationsAtBlock(
		runLimit,
		requestJobChunkLimit,
		requestJobChunkThrottleMilliseconds,
		requestJobChunkFailureLimit,
		block,
		db,
		chainClient,
		conf,
	)

	if err != nil {
		config.Log.Errorf("Failed to update delegator powers. Err: %v", err)
		return err
	}

	return nil
}

func getAndUpdateValidatorsAtBlock(blockModel *models.Block, db *gorm.DB, probeClient *client.ChainClient, conf *config.Probe) (*models.Block, []types.Validator, map[uint]database.ValidatorAmountStaked, error) {

	validators, err := requests.GetValidatorsAtHeight(probeClient, blockModel.Height)
	if err != nil {
		config.Log.Errorf("Failed to get validators at height %d. Err: %v", blockModel.Height, err)
		return nil, nil, nil, err
	}

	var validatorModels []types.Validator
	var validatorsAmountStaked map[uint]database.ValidatorAmountStaked

	err = db.Transaction(func(tx *gorm.DB) error {
		var foundValidators []types.Validator
		var validatorsAmountStakedFound = make(map[uint]database.ValidatorAmountStaked)
		var stakesToStore []database.ValidatorAmountStaked
		for _, val := range validators {
			validatorAddressModel, err := database.TranslateAndFindOrCreateValidatorByAccountAddress(db, val.OperatorAddress, conf.AccountPrefix)

			if err != nil {
				config.Log.Errorf("Failed to find or create validator. Err: %v", err)
				return err
			}

			validatorAmountStaked := database.ValidatorAmountStaked{
				ValidatorID: validatorAddressModel.ID,
				Amount:      val.Tokens.String(),
				BlockID:     blockModel.ID,
			}
			validatorsAmountStakedFound[validatorAddressModel.ID] = validatorAmountStaked
			foundValidators = append(foundValidators, val)
			stakesToStore = append(stakesToStore, validatorAmountStaked)
		}

		if len(stakesToStore) == 0 {
			return nil
		}

		// create the stakes
		err = db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "validator_id"}, {Name: "block_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"amount"}),
		}).Create(&stakesToStore).Error

		if err != nil {
			return err
		}

		validatorModels = foundValidators
		validatorsAmountStaked = validatorsAmountStakedFound
		return nil
	})

	if err != nil {
		return nil, nil, nil, err
	}

	return blockModel, validatorModels, validatorsAmountStaked, err
}

type validatorPower struct {
	amountStaked      cosmosMath.Int
	subtractionAmount cosmosMath.Int
	voteOption        database.VoteOption
}

type delegatorPower struct {
	amountDelegated cosmosMath.Int
	voteOption      database.VoteOption
}

// TODO: Optimize this later
// Two ways currently to optimize:
// Get all votes from the max height and work backwards
func UpdateProposalStatusForHeights(db *gorm.DB, blocks []models.Block, proposalID uint) error {
	config.Log.Infof("-------START UPDATE PROPOSAL STATUS FOR HEIGHTS-------")
	config.Log.Infof("Updating proposal status for proposal %d at heights", proposalID)

	// Loop backwards through the heights so that we may cache expensive queries
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Height > blocks[j].Height
	})

	if len(blocks) == 0 {
		config.Log.Infof("No blocks to update proposal status for proposal %d", proposalID)
		return nil
	}

	voteOptions, err := database.GetVoteOptionsForProposalBeforeHeightWithValidatorAccount(db, blocks[0].Height, nil, proposalID)

	// var initialHeight *int64
	// Get votes for proposal less than height, joined on validator table account_address_id

	for i, block := range blocks {
		currHeight := block.Height
		config.Log.Infof("Updating proposal status for proposal %d at height %d", proposalID, currHeight)
		config.Log.Infof("Gathering votes for proposal %d before height %d (attaching validators if exist)", proposalID, currHeight)

		if err != nil {
			config.Log.Errorf("Failed to update proposal status for proposal %d at height %d: %v", proposalID, currHeight, err)
			return err
		}

		config.Log.Infof("Gathered %d votes for proposal %d before height %d", len(voteOptions), proposalID, currHeight)

		validatorIDsMap := make(map[uint]validatorPower)
		delegatorIDsMaps := make(map[uint]delegatorPower)

		var validatorIDs []uint
		var delegatorIDs []uint

		for _, vote := range voteOptions {
			if vote.ValidatorID != 0 {
				validatorIDsMap[vote.ValidatorID] = validatorPower{
					voteOption: vote.Option,
				}
				validatorIDs = append(validatorIDs, vote.ValidatorID)
			} else {
				delegatorIDsMaps[vote.AddressID] = delegatorPower{
					voteOption: vote.Option,
				}
				delegatorIDs = append(delegatorIDs, vote.AddressID)
			}
		}

		config.Log.Infof("Gathering staking information for proposal on %d validators and %d non-validators", len(validatorIDs), len(delegatorIDs))

		proposalStatus := getZeroedProposalStatus()

		if len(delegatorIDs) != 0 {
			config.Log.Infof("Gathering delegations for delegators")
			delegatorDelegations, err := database.GetHighestBlockDelegationSnapshotsForAddresses(db, delegatorIDs)
			if err != nil {
				return err
			}
			config.Log.Infof("Gathered %d delegations for validators", len(delegatorDelegations))

			for _, delegation := range delegatorDelegations {

				res, ok := cosmosMath.NewIntFromString(delegation.Amount)

				if !ok {
					config.Log.Errorf("Failed to convert amount to cosmos math int for adding: %s", delegation.Amount)
					return errors.New("math detected overflow in string parsing")
				}

				if _, ok := validatorIDsMap[delegation.ValidatorID]; ok {
					val := validatorIDsMap[delegation.ValidatorID]
					if val.subtractionAmount.IsNil() {
						val.subtractionAmount = cosmosMath.ZeroInt()
					}
					val.subtractionAmount = val.subtractionAmount.Add(res)
					validatorIDsMap[delegation.ValidatorID] = val
				}

				if _, ok := delegatorIDsMaps[delegation.AddressID]; ok {
					del := delegatorIDsMaps[delegation.AddressID]
					if del.amountDelegated.IsNil() {
						del.amountDelegated = cosmosMath.ZeroInt()
					}
					del.amountDelegated = del.amountDelegated.Add(res)
					delegatorIDsMaps[delegation.AddressID] = del

					proposalStatus[del.voteOption] = proposalStatus[del.voteOption].Add(res)
				}
			}

		} else {
			config.Log.Infof("No delegations to gather")
		}

		if len(validatorIDs) != 0 {
			config.Log.Infof("Gathering staking information for proposal on %d validators", len(validatorIDs))
			validatorAmounts, err := database.GetHighestBlockAmountStakedForValidators(db, validatorIDs)

			if err != nil {
				return err
			}

			for _, validatorAmount := range validatorAmounts {
				res, ok := cosmosMath.NewIntFromString(validatorAmount.Amount)

				if !ok {
					config.Log.Errorf("Failed to convert amount to cosmos math int for adding: %s", validatorAmount.Amount)
					return errors.New("math detected overflow in string parsing")
				}

				if _, ok := validatorIDsMap[validatorAmount.ValidatorID]; ok {
					val := validatorIDsMap[validatorAmount.ValidatorID]
					val.amountStaked = res
					if !val.subtractionAmount.IsNil() {
						val.amountStaked = val.amountStaked.Sub(val.subtractionAmount)
					}
					validatorIDsMap[validatorAmount.ValidatorID] = val

					proposalStatus[val.voteOption] = proposalStatus[val.voteOption].Add(val.amountStaked)
				}
			}
		}

		config.Log.Infof("Updating proposal status for proposal %d at height %d", proposalID, currHeight)
		config.Log.Infof("Proposal has statuses: Yes (%s), No (%s), Abstain (%s), Veto (%s), Empty (%s)", proposalStatus[database.Yes].String(), proposalStatus[database.No].String(), proposalStatus[database.Abstain].String(), proposalStatus[database.Veto].String(), proposalStatus[database.Empty].String())

		proposalStatusModel := database.ProposalStatus{
			ProposalID:    proposalID,
			BlockID:       block.ID,
			YesAmount:     decimal.NewFromBigInt(proposalStatus[database.Yes].BigInt(), 0),
			NoAmount:      decimal.NewFromBigInt(proposalStatus[database.No].BigInt(), 0),
			AbstainAmount: decimal.NewFromBigInt(proposalStatus[database.Abstain].BigInt(), 0),
			VetoAmount:    decimal.NewFromBigInt(proposalStatus[database.Veto].BigInt(), 0),
			EmptyAmount:   decimal.NewFromBigInt(proposalStatus[database.Empty].BigInt(), 0),
		}

		err = db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "proposal_id"}, {Name: "block_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"yes_amount", "no_amount", "abstain_amount", "veto_amount", "empty_amount"}),
		}).Create(&proposalStatusModel).Error

		if err != nil {
			return err
		}

		var remainingVoteOptions []database.VotesForProposalBeforeHeightWithValidatorAccountScanResult

		if i != len(blocks)-1 {
			for _, voteOption := range voteOptions {
				if voteOption.Height <= blocks[i+1].Height {
					remainingVoteOptions = append(remainingVoteOptions, voteOption)
				}
			}

			if len(remainingVoteOptions) == len(voteOptions) {
				config.Log.Infof("No votes to eliminate for proposal %d that came after next height %d", proposalID, blocks[i+1].Height)
			} else {
				config.Log.Infof("Eliminated %d/%d votes for proposal %d that came after next height %d", len(voteOptions)-len(remainingVoteOptions), len(voteOptions), proposalID, blocks[i+1].Height)
			}

			voteOptions = remainingVoteOptions
		}

		config.Log.Infof("Updating proposal status for proposal %d at height %d complete", proposalID, currHeight)

		// TODO: Use this later for optimizing the loop since its currently non-optimal
		// initialHeight = &currHeight
	}
	config.Log.Infof("-------END UPDATE PROPOSAL STATUS FOR HEIGHTS-------")
	return nil

}

var finishedOnchainProposalsMap = make(map[uint64]*requests.OnChainProposal)
var finshedMapMutex = sync.Mutex{}

func GetOrAddFinishedOnchainProposal(proposalID uint64, lcdURL string) (*requests.OnChainProposal, error) {
	if proposal, ok := finishedOnchainProposalsMap[proposalID]; ok {
		return proposal, nil
	}

	proposal, err := requests.GetOnChainProposalByID(proposalID, lcdURL)

	if err != nil {
		return nil, err
	}

	// Basic heuristic to ensure proposal has finished being tallied, should not happen
	if tallyIsEmpty(proposal.FinalTallyResult) {
		// Proposal has not finished being tallied, do NOT set in the map
		config.Log.Infof("Proposal %d has not finished being tallied, returning nil", proposalID)
		return nil, nil
	}

	finshedMapMutex.Lock()
	defer finshedMapMutex.Unlock()

	finishedOnchainProposalsMap[proposalID] = proposal

	return proposal, nil
}

func tallyIsEmpty(tally requests.OnChainFinalTally) bool {
	return tallyValueIsEmpty(tally.Abstain) && tallyValueIsEmpty(tally.No) && tallyValueIsEmpty(tally.Yes) && tallyValueIsEmpty(tally.NoWithVeto)
}

func tallyValueIsEmpty(tally string) bool {
	return tally == "0" || tally == ""
}

func UpdateProposalStatusAtBlock(interval int64, blocks *models.Block, onchainProposalsToVotingEndTime map[uint64]time.Time, db *gorm.DB, lcdURL string) {
	var proposals []database.Proposal
	err := db.Table("proposals").Select("id, proposal_id").Find(&proposals).Error

	if err != nil {
		config.Log.Errorf("Failed to get all proposals. Err: %v", err)
		return
	}

	for _, proposal := range proposals {

		votingEndTime, ok := onchainProposalsToVotingEndTime[proposal.ProposalID]

		if !ok {
			config.Log.Errorf("Failed to get voting end time for proposal %d, not in map", proposal.ProposalID)
			continue
		}

		if blocks.TimeStamp.After(votingEndTime) {
			config.Log.Infof("Proposal %d has ended voting at block %d", proposal.ProposalID, blocks.Height)

			onchainProposal, err := GetOrAddFinishedOnchainProposal(proposal.ProposalID, lcdURL)

			if err != nil {
				config.Log.Errorf("Failed to get onchain proposal for proposal %d at block %d. Err: %v", proposal.ProposalID, blocks.Height, err)
				continue
			}

			if onchainProposal == nil {
				config.Log.Infof("Proposal %d has not finished being tallied at block %d, calculating internal result from indexed data", proposal.ProposalID, blocks.Height)

				err := UpdateProposalStatusForHeights(db, []models.Block{*blocks}, uint(proposal.ID))

				if err != nil {
					config.Log.Errorf("Failed to update proposal status for proposal %d at block %d. Err: %v", proposal.ID, blocks.Height, err)
				}
			} else {
				config.Log.Infof("Proposal %d has finished being tallied at block %d. Creating status entry for final tally instead of calculating.", proposal.ProposalID, blocks.Height)

				yes, ok := cosmosMath.NewIntFromString(onchainProposal.FinalTallyResult.Yes)
				if !ok {
					config.Log.Fatalf("Failed to convert amount to cosmos math int for adding: %s", onchainProposal.FinalTallyResult.Yes)
				}

				abstain, ok := cosmosMath.NewIntFromString(onchainProposal.FinalTallyResult.Abstain)

				if !ok {
					config.Log.Fatalf("Failed to convert amount to cosmos math int for adding: %s", onchainProposal.FinalTallyResult.Abstain)
				}

				no, ok := cosmosMath.NewIntFromString(onchainProposal.FinalTallyResult.No)

				if !ok {
					config.Log.Fatalf("Failed to convert amount to cosmos math int for adding: %s", onchainProposal.FinalTallyResult.No)
				}

				veto, ok := cosmosMath.NewIntFromString(onchainProposal.FinalTallyResult.NoWithVeto)

				if !ok {
					config.Log.Fatalf("Failed to convert amount to cosmos math int for adding: %s", onchainProposal.FinalTallyResult.NoWithVeto)
				}

				proposalStatusModel := database.ProposalStatus{
					ProposalID:    uint(proposal.ID),
					BlockID:       blocks.ID,
					YesAmount:     decimal.NewFromBigInt(yes.BigInt(), 0),
					NoAmount:      decimal.NewFromBigInt(no.BigInt(), 0),
					AbstainAmount: decimal.NewFromBigInt(abstain.BigInt(), 0),
					VetoAmount:    decimal.NewFromBigInt(veto.BigInt(), 0),
					EmptyAmount:   decimal.NewFromInt(0),
				}

				err = db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "proposal_id"}, {Name: "block_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"yes_amount", "no_amount", "abstain_amount", "veto_amount", "empty_amount"}),
				}).Create(&proposalStatusModel).Error

				if err != nil {
					config.Log.Fatalf("Failed to create proposal status for proposal %d at block %d. Err: %v", proposal.ID, blocks.Height, err)
				}
			}
		} else {
			config.Log.Infof("Proposal %d is still in voting period at block %d", proposal.ProposalID, blocks.Height)
			err := UpdateProposalStatusForHeights(db, []models.Block{*blocks}, uint(proposal.ID))

			if err != nil {
				config.Log.Errorf("Failed to update proposal status for proposal %d at block %d. Err: %v", proposal.ID, blocks.Height, err)
			}
		}
	}
}

func getZeroedProposalStatus() map[database.VoteOption]cosmosMath.Int {
	proposalStatus := make(map[database.VoteOption]cosmosMath.Int)
	for _, option := range []database.VoteOption{database.Yes, database.Empty, database.Abstain, database.No, database.Veto} {
		proposalStatus[option] = cosmosMath.ZeroInt()
	}

	return proposalStatus
}
