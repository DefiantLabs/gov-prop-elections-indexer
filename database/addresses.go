package database

import "gorm.io/gorm"

type CountDistinctAddressesScanResult struct {
	ProposalID uint64 `json:"proposal_id"`
	Count      int    `json:"count"`
}

func CountDistinctAddressesByProposal(db *gorm.DB, proposals *[]Proposal) ([]CountDistinctAddressesScanResult, error) {
	var scanResult []CountDistinctAddressesScanResult

	initialQuery := db.Table("votes").
		Joins("LEFT JOIN proposals on votes.proposal_id=proposals.id").
		Select(`proposals.proposal_id as proposal_id,
				COUNT(votes.address_id) as count`).
		Group("proposals.proposal_id")

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

func CountDistinctAddresses(db *gorm.DB) (int, error) {
	var count int

	err := db.Table("votes").
		Select("COUNT(DISTINCT address_id)").
		Scan(&count).Error

	if err != nil {
		return 0, err
	}

	return count, nil
}
