package main

import (
	"errors"
	"sync"

	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/gov-prop-elections-indexer/appconfig"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"

	"github.com/DefiantLabs/cosmos-indexer/config"
	indexerTxTypes "github.com/DefiantLabs/cosmos-indexer/cosmos/modules/tx"
	dbTypes "github.com/DefiantLabs/cosmos-indexer/db"
	"github.com/DefiantLabs/cosmos-indexer/parsers"
	stdTypes "github.com/cosmos/cosmos-sdk/types"
	authz "github.com/cosmos/cosmos-sdk/x/authz"
	govV1 "github.com/cosmos/cosmos-sdk/x/gov/types/v1"
	govV1Beta1 "github.com/cosmos/cosmos-sdk/x/gov/types/v1beta1"
	"gorm.io/gorm"
)

// This defines the custom message parser for the governance vote message type
// It implements the MessageParser interface
type MsgVoteParser struct {
	Id                     string
	ProposalAllowlist      map[uint64]bool
	Initialized            bool
	ProposalsConfig        *appconfig.ProposalsConfig
	proposalAllowlistMutex sync.Mutex
}

func (c *MsgVoteParser) Identifier() string {
	return c.Id
}

// Dumb hack, need a way to intitialize the proposal allowlist from the config before indexing starts, even though the config args are loaded after the indexer is initialized
func (c *MsgVoteParser) setProposalAllowlist() {
	c.proposalAllowlistMutex.Lock()
	defer c.proposalAllowlistMutex.Unlock()

	c.ProposalAllowlist = make(map[uint64]bool)
	if c.ProposalsConfig.ProposalAllowList != nil {
		for _, proposalID := range c.ProposalsConfig.ProposalAllowList {
			upcast := uint64(proposalID)
			c.ProposalAllowlist[upcast] = true
		}
	}

	if c.ProposalsConfig.ClydeProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.ClydeProposal)] = true
	}

	if c.ProposalsConfig.GraceProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.GraceProposal)] = true
	}

	if c.ProposalsConfig.MattProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.MattProposal)] = true
	}

	c.Initialized = true
}

func (c *MsgVoteParser) ParseMessage(cosmosMsg stdTypes.Msg, log *indexerTxTypes.LogMessage, cfg config.IndexConfig) (*any, error) {

	if !c.Initialized {
		c.setProposalAllowlist()
	}

	if len(c.ProposalAllowlist) == 0 {
		config.Log.Fatal("Proposal allowlist is empty")
	}

	msgV1Beta1, okV1Beta1 := cosmosMsg.(*govV1Beta1.MsgVote)
	msgV1, okV1 := cosmosMsg.(*govV1.MsgVote)

	if !okV1Beta1 && !okV1 {
		return nil, errors.New("not a vote message")
	}

	var val database.Vote

	if okV1Beta1 {
		val = database.Vote{
			Option: convertV1Beta1VoteOption(msgV1Beta1.Option),
			Address: models.Address{
				Address: msgV1Beta1.Voter,
			},
			Proposal: database.Proposal{
				ProposalID: msgV1Beta1.ProposalId,
			},
		}
	} else {
		val = database.Vote{
			Option: convertV1VoteOption(msgV1.Option),
			Address: models.Address{
				Address: msgV1.Voter,
			},
			Proposal: database.Proposal{
				ProposalID: msgV1.ProposalId,
			},
		}
	}

	if allowed, ok := c.ProposalAllowlist[val.Proposal.ProposalID]; !ok || !allowed {
		config.Log.Infof("Found vote for Proposal %d, not in allowlist, skipping vote indexing", val.Proposal.ProposalID)
		return nil, nil
	}

	config.Log.Infof("Found vote for Proposal %d, in allowlist, indexing vote", val.Proposal.ProposalID)

	storageVal := any(val)

	return &storageVal, nil
}

func convertV1Beta1VoteOption(option govV1Beta1.VoteOption) database.VoteOption {
	switch option {
	case govV1Beta1.OptionYes:
		return database.Yes
	case govV1Beta1.OptionNo:
		return database.No
	case govV1Beta1.OptionAbstain:
		return database.Abstain
	case govV1Beta1.OptionNoWithVeto:
		return database.Veto
	case govV1Beta1.OptionEmpty:
		return database.Empty
	default:
		return -1
	}
}

func convertV1VoteOption(option govV1.VoteOption) database.VoteOption {
	switch option {
	case govV1.OptionYes:
		return database.Yes
	case govV1.OptionNo:
		return database.No
	case govV1.OptionAbstain:
		return database.Abstain
	case govV1.OptionNoWithVeto:
		return database.Veto
	case govV1.OptionEmpty:
		return database.Empty
	default:
		return -1
	}
}

// This method is called during database insertion. It is responsible for storing the parsed data in the database.
// The gorm db is wrapped in a transaction, so any errors will cause a rollback.
// Any errors returned will be saved as a parser error in the database as well for later debugging.
func (c *MsgVoteParser) IndexMessage(dataset *any, db *gorm.DB, message models.Message, messageEvents []parsers.MessageEventWithAttributes, cfg config.IndexConfig) error {
	vote, ok := (*dataset).(database.Vote)

	if !ok {
		return errors.New("invalid vote type")
	}

	// Find the address in the database
	var err error
	var voter models.Address
	voter, err = dbTypes.FindOrCreateAddressByAddress(db, vote.Address.Address)

	if err != nil {
		return err
	}

	var proposal database.Proposal
	err = db.Where(&database.Proposal{ProposalID: vote.Proposal.ProposalID}).FirstOrCreate(&proposal).Error

	if err != nil {
		return err
	}

	vote.BlockID = message.Tx.Block.ID
	vote.Block = message.Tx.Block
	vote.Tx = message.Tx
	vote.TxID = message.Tx.ID
	vote.AddressID = voter.ID
	vote.Address = voter
	vote.ProposalID = proposal.ID
	vote.Proposal = proposal

	var indexedVoteSearch database.Vote

	err = db.Where(&database.Vote{ProposalID: proposal.ID, AddressID: voter.ID}).Find(&indexedVoteSearch).Error

	if err != nil {
		return err
	}

	if indexedVoteSearch.ID == 0 {
		err = db.Create(&vote).Error
	} else {
		if vote.Block.Height >= indexedVoteSearch.Block.Height {
			err = db.Model(&indexedVoteSearch).Updates(&database.Vote{
				Block:   vote.Block,
				BlockID: vote.BlockID,
				Option:  vote.Option,
			}).Error

			if err != nil {
				return err
			}

			vote.ID = indexedVoteSearch.ID
		} else {
			vote.ID = indexedVoteSearch.ID
			vote.AddressID = indexedVoteSearch.AddressID
			vote.ProposalID = indexedVoteSearch.ProposalID
			vote.TxID = indexedVoteSearch.TxID
			vote.BlockID = indexedVoteSearch.BlockID
		}
	}

	anyRecast := any(vote)

	*dataset = anyRecast

	return err
}

type MsgAuthzExec struct {
	Id                     string
	ProposalAllowlist      map[uint64]bool
	Initialized            bool
	ProposalsConfig        *appconfig.ProposalsConfig
	proposalAllowlistMutex sync.Mutex
}

func (c *MsgAuthzExec) Identifier() string {
	return c.Id
}

// Dumb hack, need a way to intitialize the proposal allowlist from the config before indexing starts, even though the config args are loaded after the indexer is initialized
func (c *MsgAuthzExec) setProposalAllowlist() {
	c.proposalAllowlistMutex.Lock()
	defer c.proposalAllowlistMutex.Unlock()

	c.ProposalAllowlist = make(map[uint64]bool)
	if c.ProposalsConfig.ProposalAllowList != nil {
		for _, proposalID := range c.ProposalsConfig.ProposalAllowList {
			upcast := uint64(proposalID)
			c.ProposalAllowlist[upcast] = true
		}
	}

	if c.ProposalsConfig.ClydeProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.ClydeProposal)] = true
	}

	if c.ProposalsConfig.GraceProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.GraceProposal)] = true
	}

	if c.ProposalsConfig.MattProposal != 0 {
		c.ProposalAllowlist[uint64(c.ProposalsConfig.MattProposal)] = true
	}

	c.Initialized = true
}

func (c *MsgAuthzExec) ParseMessage(cosmosMsg stdTypes.Msg, log *indexerTxTypes.LogMessage, cfg config.IndexConfig) (*any, error) {

	if !c.Initialized {
		c.setProposalAllowlist()
	}

	if len(c.ProposalAllowlist) == 0 {
		config.Log.Fatal("Proposal allowlist is empty")
	}

	msgExec, okMsgExec := cosmosMsg.(*authz.MsgExec)

	if !okMsgExec {
		return nil, errors.New("not a exec message")
	}
	var ret []database.Vote
	for _, msg := range msgExec.Msgs {
		msgV1Beta1, okV1Beta1 := msg.GetCachedValue().(*govV1Beta1.MsgVote)

		if okV1Beta1 && msg.TypeUrl != MsgVoteV1Beta1 {
			return nil, errors.New("not a vote message based on typeurl check")
		}

		if !okV1Beta1 && msg.TypeUrl == MsgVoteV1Beta1 {
			return nil, errors.New("not a vote message based on typeurl check")
		}

		if okV1Beta1 {
			config.Log.Infof("Found vote message in exec message")
			val := database.Vote{
				Option: convertV1Beta1VoteOption(msgV1Beta1.Option),
				Address: models.Address{
					Address: msgV1Beta1.Voter,
				},
				Proposal: database.Proposal{
					ProposalID: msgV1Beta1.ProposalId,
				},
			}

			if allowed, ok := c.ProposalAllowlist[val.Proposal.ProposalID]; !ok || !allowed {
				config.Log.Infof("Found vote for Proposal %d, not in allowlist, skipping vote indexing", val.Proposal.ProposalID)
				continue
			}

			config.Log.Infof("Found vote for Proposal %d, in allowlist, indexing vote", val.Proposal.ProposalID)
			ret = append(ret, val)
			continue
		}

		msgV1, okV1 := msg.GetCachedValue().(*govV1.MsgVote)

		if okV1 && msg.TypeUrl != MsgVoteV1 {
			return nil, errors.New("not a vote message based on typeurl check")
		}

		if !okV1 && msg.TypeUrl == MsgVoteV1 {
			return nil, errors.New("not a vote message based on typeurl check")
		}

		if okV1 {
			config.Log.Infof("Found vote message in exec message")
			val := database.Vote{
				Option: convertV1VoteOption(msgV1.Option),
				Address: models.Address{
					Address: msgV1.Voter,
				},
				Proposal: database.Proposal{
					ProposalID: msgV1.ProposalId,
				},
			}

			if allowed, ok := c.ProposalAllowlist[val.Proposal.ProposalID]; !ok || !allowed {
				config.Log.Infof("Found vote for Proposal %d, not in allowlist, skipping vote indexing", val.Proposal.ProposalID)
				continue
			}

			config.Log.Infof("Found vote for Proposal %d, in allowlist, indexing vote", val.Proposal.ProposalID)

			ret = append(ret, val)
			continue
		}

	}

	if len(ret) == 0 {
		return nil, nil
	}

	storageVal := any(ret)

	return &storageVal, nil
}

func (c *MsgAuthzExec) IndexMessage(dataset *any, db *gorm.DB, message models.Message, messageEvents []parsers.MessageEventWithAttributes, cfg config.IndexConfig) error {
	votes, ok := (*dataset).([]database.Vote)

	if !ok {
		return errors.New("invalid vote type")
	}

	for i, vote := range votes {
		// Find the address in the database
		var err error
		var voter models.Address
		voter, err = dbTypes.FindOrCreateAddressByAddress(db, vote.Address.Address)

		if err != nil {
			return err
		}

		var proposal database.Proposal
		err = db.Where(&database.Proposal{ProposalID: vote.Proposal.ProposalID}).FirstOrCreate(&proposal).Error

		if err != nil {
			return err
		}

		vote.BlockID = message.Tx.Block.ID
		vote.Block = message.Tx.Block
		vote.Tx = message.Tx
		vote.TxID = message.Tx.ID
		vote.AddressID = voter.ID
		vote.Address = voter
		vote.ProposalID = proposal.ID
		vote.Proposal = proposal

		var indexedVoteSearch database.Vote

		err = db.Where(&database.Vote{ProposalID: proposal.ID, AddressID: voter.ID}).Find(&indexedVoteSearch).Error

		if err != nil {
			return err
		}

		if indexedVoteSearch.ID == 0 {
			err = db.Create(&vote).Error

			if err != nil {
				return err
			}
		} else {
			if vote.Block.Height >= indexedVoteSearch.Block.Height {
				err = db.Model(&indexedVoteSearch).Updates(&database.Vote{
					Block:   vote.Block,
					BlockID: vote.BlockID,
					Option:  vote.Option,
				}).Error

				if err != nil {
					return err
				}

				vote.ID = indexedVoteSearch.ID
			} else {
				vote.ID = indexedVoteSearch.ID
				vote.AddressID = indexedVoteSearch.AddressID
				vote.ProposalID = indexedVoteSearch.ProposalID
				vote.TxID = indexedVoteSearch.TxID
				vote.BlockID = indexedVoteSearch.BlockID
			}
		}

		votes[i] = vote
	}

	anyRecast := any(votes)

	*dataset = anyRecast

	return nil
}
