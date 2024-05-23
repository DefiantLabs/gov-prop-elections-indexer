package database

import (
	"time"

	"gorm.io/gorm"
)

type GetVotesPaginatedScanResult struct {
	Option       VoteOption `json:"-"`
	Height       int64      `json:"height"`
	Address      string     `json:"address"`
	ProposalID   uint64     `json:"proposal_id"`
	TimeStamp    time.Time  `json:"time_stamp"`
	Hash         string     `json:"hash"`
	OptionString string     `json:"option" gorm:"-"`
}

func GetVotesPaginated(db *gorm.DB, currentOffset int, currentLimit int, proposals []Proposal) ([]GetVotesPaginatedScanResult, error) {
	// Get votes from the database
	var votes []GetVotesPaginatedScanResult

	initialQuery := db.Table("votes").Joins("LEFT JOIN proposals on votes.proposal_id=proposals.id").
		Joins("LEFT JOIN blocks on votes.block_id=blocks.id").
		Joins("LEFT JOIN addresses on votes.address_id=addresses.id").
		Joins("LEFT JOIN txes on votes.tx_id=txes.id").
		Order("blocks.height DESC").
		Offset(currentOffset).
		Limit(currentLimit).
		Select(`
			votes.option as option,
			blocks.height as height,
			blocks.time_stamp as time_stamp,
			addresses.address as address,
			proposals.proposal_id as proposal_id,
			txes.hash
		`).
		Order("proposals.proposal_id DESC")

	if len(proposals) > 0 {
		var proposalIDs []uint64

		for _, proposal := range proposals {
			proposalIDs = append(proposalIDs, proposal.ProposalID)
		}

		initialQuery = initialQuery.Where("proposals.proposal_id IN ?", proposalIDs)
	}

	err := initialQuery.Scan(&votes).Error

	if err != nil {
		return nil, err
	}

	for i := range votes {
		votes[i].OptionString = votes[i].Option.String()
	}

	return votes, nil
}

type DistinctVoteOptionCounts struct {
	EmptyCount   int64                                   `json:"empty_count"`
	YesCount     int64                                   `json:"yes_count"`
	AbstainCount int64                                   `json:"abstain_count"`
	NoCount      int64                                   `json:"no_count"`
	VetoCount    int64                                   `json:"veto_count"`
	Proposals    map[uint64]CountDistinctVotesScanResult `json:"proposals"`
}

type CountDistinctVotesScanResult struct {
	ProposalID   uint64     `json:"-"`
	EmptyCount   VoteOption `json:"empty_count"`
	YesCount     VoteOption `json:"yes_count"`
	AbstainCount VoteOption `json:"abstain_count"`
	NoCount      VoteOption `json:"no_count"`
	VetoCount    VoteOption `json:"veto_count"`
}

func CountDistinctVoteOptionsByProposals(db *gorm.DB, proposals *[]Proposal) ([]CountDistinctVotesScanResult, error) {

	var scanResult []CountDistinctVotesScanResult

	initialQuery := db.Table("votes").
		Joins("LEFT JOIN proposals on votes.proposal_id=proposals.id").
		Select(`proposals.proposal_id as proposal_id,
				count(CASE WHEN votes.option = 0 THEN 1 ELSE NULL END) empty_count,
				count(CASE WHEN votes.option = 1 THEN 1 ELSE NULL END) yes_count,
				count(CASE WHEN votes.option = 2 THEN 1 ELSE NULL END) abstain_count,
				count(CASE WHEN votes.option = 2 THEN 1 ELSE NULL END) no_count,
				count(CASE WHEN votes.option = 3 THEN 1 ELSE NULL END) veto_count`).
		Group("proposals.proposal_id").Order("proposal_id ASC")

	if proposals != nil {
		var proposalIDs []uint64

		for _, proposal := range *proposals {
			proposalIDs = append(proposalIDs, proposal.ID)
		}

		initialQuery = initialQuery.Where("votes.proposal_id IN ?", proposalIDs)
	}

	err := initialQuery.Scan(&scanResult).Error

	if err != nil {
		return nil, err
	}

	return scanResult, nil
}

type GroupVotesByProposalAndBlockResponse struct {
	ProposalID   *uint64 `json:"-"`
	Height       int64   `json:"height"`
	EmptyCount   int     `json:"empty_count"`
	YesCount     int     `json:"yes_count"`
	AbstainCount int     `json:"abstain_count"`
	NoCount      int     `json:"no_count"`
	VetoCount    int     `json:"veto_count"`
}

func GroupVotesByProposalAndBlock(db *gorm.DB, proposals *[]Proposal) ([]GroupVotesByProposalAndBlockResponse, error) {

	var scanResult []GroupVotesByProposalAndBlockResponse

	// libpq does not handle array_agg well, so we hack it with count cases
	initialQuery := db.Table("votes").
		Joins("LEFT JOIN proposals on votes.proposal_id=proposals.id").
		Joins("RIGHT JOIN blocks on votes.block_id=blocks.id").
		Select(`proposals.proposal_id as proposal_id,
				blocks.height as height,
				count(CASE WHEN votes.option = 0 THEN 1 ELSE NULL END) empty_count,
				count(CASE WHEN votes.option = 1 THEN 1 ELSE NULL END) yes_count,
				count(CASE WHEN votes.option = 2 THEN 1 ELSE NULL END) abstain_count,
				count(CASE WHEN votes.option = 3 THEN 1 ELSE NULL END) no_count,
				count(CASE WHEN votes.option = 4 THEN 1 ELSE NULL END) veto_count`).
		Group("proposals.proposal_id, blocks.height").Order("height ASC, proposal_id ASC")

	if proposals != nil {

		var proposalIDs []uint64

		for _, proposal := range *proposals {
			proposalIDs = append(proposalIDs, proposal.ID)
		}

		initialQuery = initialQuery.Where("votes.proposal_id IN ? OR proposals.proposal_id is NULL", proposalIDs)
	}

	err := initialQuery.Scan(&scanResult).Error

	if err != nil {
		return nil, err
	}

	return scanResult, nil
}

type EarliestProposalVotesScanResult struct {
	ProposalID uint  `json:"proposal_id"`
	Height     int64 `json:"height"`
}

func GetEarliestVoteForAllProposals(db *gorm.DB) ([]EarliestProposalVotesScanResult, error) {

	earliestVotes := make([]EarliestProposalVotesScanResult, 0)
	err := db.Table("votes").Select("votes.proposal_id, min(blocks.height) as height").
		Joins("JOIN blocks on blocks.id=votes.block_id").
		Group("votes.proposal_id").Scan(&earliestVotes).Error

	if err != nil {
		return nil, err
	}

	return earliestVotes, nil
}

type VotesForProposalBeforeHeightWithValidatorAccountScanResult struct {
	AddressID   uint
	Option      VoteOption
	ValidatorID uint
	Height      int64
}

func GetVoteOptionsForProposalBeforeHeightWithValidatorAccount(db *gorm.DB, height int64, startHeight *int64, proposal uint) ([]VotesForProposalBeforeHeightWithValidatorAccountScanResult, error) {
	var scanResult []VotesForProposalBeforeHeightWithValidatorAccountScanResult
	initQuery := db.Table("votes").
		Joins("JOIN blocks on blocks.id = votes.block_id").
		Joins("LEFT JOIN validators on votes.address_id = validators.account_address_id").
		Select("votes.address_id, votes.option, validators.id as validator_id, blocks.height as height")

	if startHeight != nil {
		initQuery = initQuery.Where("blocks.height >= ? AND blocks.height <= ? AND proposal_id = ?", startHeight, height, proposal)
	} else {
		initQuery = initQuery.Where("blocks.height < ? AND proposal_id = ?", height, proposal)
	}

	err := initQuery.Scan(&scanResult).Error

	return scanResult, err
}
