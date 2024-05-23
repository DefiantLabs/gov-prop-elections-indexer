package database

import (
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type ProposalStatusBeforeHeightScanResult struct {
	Height     int64 `json:"height"`
	ProposalID uint  `json:"proposal_id"`
}

func GetProposalStatusesBeforeHeightByProposalID(db *gorm.DB, height int64, proposalID []uint) ([]ProposalStatusBeforeHeightScanResult, error) {

	var scanResult []ProposalStatusBeforeHeightScanResult

	err := db.Table("proposal_statuses").
		Select("blocks.height, proposal_statuses.proposal_id").
		Joins("JOIN blocks on proposal_statuses.block_id = blocks.id").
		Where("proposal_statuses.proposal_id in (?) AND blocks.height <= ?", proposalID, height).
		Group("proposal_statuses.proposal_id, blocks.height").
		Order("blocks.height ASC").
		Scan(&scanResult).Error

	if err != nil {
		return nil, err
	}

	return scanResult, nil
}

type ProposalStatusByProposalID struct {
	ProposalID    uint            `json:"proposal_id"`
	Height        uint            `json:"height"`
	TimeStamp     time.Time       `json:"time_stamp"`
	YesAmount     decimal.Decimal `json:"yes_amount"`
	NoAmount      decimal.Decimal `json:"no_amount"`
	EmptyAmount   decimal.Decimal `json:"empty_amount"`
	AbstainAmount decimal.Decimal `json:"abstain_amount"`
	VetoAmount    decimal.Decimal `json:"veto_amount"`
}

func GetProposalStatusesByProposalID(db *gorm.DB, proposalID []uint) ([]ProposalStatusByProposalID, error) {

	var scanResult []ProposalStatusByProposalID

	err := db.Table("proposal_statuses").
		Select("blocks.height, blocks.time_stamp, proposals.proposal_id, proposal_statuses.yes_amount, proposal_statuses.no_amount, proposal_statuses.empty_amount, proposal_statuses.abstain_amount, proposal_statuses.veto_amount").
		Joins("JOIN blocks on proposal_statuses.block_id = blocks.id").
		Joins("JOIN proposals on proposal_statuses.proposal_id = proposals.id").
		Where("proposals.proposal_id in (?)", proposalID).
		Order("blocks.height ASC").
		Scan(&scanResult).Error

	if err != nil {
		return nil, err
	}

	return scanResult, nil
}

// select proposals.proposal_id, proposal_statuses.yes_amount, proposal_statuses.no_amount, proposal_statuses.empty_amount, proposal_statuses.abstain_amount, proposal_statuses.veto_amount from proposal_statuses
// join blocks on blocks.id = proposal_statuses.block_id
// join proposals on proposals.id = proposal_statuses.proposal_id
// join (select
//     proposal_statuses.proposal_id,
//     MAX(height)
// from
//     proposal_statuses
//     join blocks on blocks.id = proposal_statuses.block_id
// where proposal_statuses.proposal_id in (1, 2, 3)
// group by
//     proposal_statuses.proposal_id
// ) max_height
// on max_height.proposal_id = proposal_statuses.proposal_id AND max_height.max = blocks.height
// ;

type LatestProposalStatusByProposalIDScanResult struct {
	ProposalID    uint            `json:"proposal_id"`
	YesAmount     decimal.Decimal `json:"yes_amount"`
	NoAmount      decimal.Decimal `json:"no_amount"`
	EmptyAmount   decimal.Decimal `json:"empty_amount"`
	AbstainAmount decimal.Decimal `json:"abstain_amount"`
	VetoAmount    decimal.Decimal `json:"veto_amount"`
}

func GetLatestProposalStatusesByProposalID(db *gorm.DB, proposalID []int) ([]LatestProposalStatusByProposalIDScanResult, error) {

	var scanResult []LatestProposalStatusByProposalIDScanResult

	subQuery := db.Table("proposal_statuses").
		Select("proposal_statuses.proposal_id, MAX(blocks.height) as max").
		Joins("JOIN blocks on blocks.id = proposal_statuses.block_id").
		Joins("JOIN proposals on proposals.id = proposal_statuses.proposal_id").
		Where("proposals.proposal_id in (?)", proposalID).
		Group("proposal_statuses.proposal_id")

	err := db.Table("proposal_statuses").
		Select("proposals.proposal_id, proposal_statuses.yes_amount, proposal_statuses.no_amount, proposal_statuses.empty_amount, proposal_statuses.abstain_amount, proposal_statuses.veto_amount").
		Joins("JOIN blocks on blocks.id = proposal_statuses.block_id").
		Joins("JOIN proposals on proposals.id = proposal_statuses.proposal_id").
		Joins("JOIN (?) max_height on max_height.proposal_id = proposal_statuses.proposal_id AND max_height.max = blocks.height", subQuery).
		Scan(&scanResult).Error

	if err != nil {
		return nil, err
	}

	return scanResult, nil
}
