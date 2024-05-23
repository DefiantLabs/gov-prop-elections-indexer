package database

import "gorm.io/gorm"

func FindProposalsByIDs(db *gorm.DB, proposalIDs []int) ([]Proposal, error) {
	var proposals []Proposal
	var err error
	if len(proposalIDs) == 0 {
		err = db.Find(&proposals).Error
	} else {
		err = db.Where("proposal_id IN ?", proposalIDs).Find(&proposals).Error
	}

	return proposals, err
}
