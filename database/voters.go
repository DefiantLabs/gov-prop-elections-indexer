package database

import "gorm.io/gorm"

type votersMissingDelegationSnapshotsScanResult struct {
	AddressID uint
}

func GetVotersMissingDelegationSnapshots(db *gorm.DB) ([]votersMissingDelegationSnapshotsScanResult, error) {
	var votersMissingDelegationsResult []votersMissingDelegationSnapshotsScanResult

	err := db.Table("votes").
		Joins("JOIN blocks on blocks.id=votes.block_id").
		Joins("JOIN addresses on addresses.id=votes.address_id").
		Joins("LEFT JOIN voter_delegation_snapshots on votes.address_id=voter_delegation_snapshots.address_id").
		Where("voter_delegation_snapshots.id IS NULL").
		Select("DISTINCT(votes.address_id), addresses.address, votes.proposal_id, blocks.height").
		Scan(&votersMissingDelegationsResult).Error

	return votersMissingDelegationsResult, err
}
