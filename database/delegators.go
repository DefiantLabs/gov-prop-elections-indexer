package database

import "gorm.io/gorm"

type OldestDelegators struct {
	AddressID uint
	Address   string
	Height    uint
}

func GetOldestVotersForDelegations(n int, db *gorm.DB) ([]OldestDelegators, error) {
	var delegations []OldestDelegators

	// Get the highest delegation heights for each voter, including nulls
	err := db.Table("voter_delegation_snapshots").
		Select("votes.address_id, addresses.address, MAX(blocks.height) as height").
		Joins("RIGHT JOIN votes ON votes.address_id=voter_delegation_snapshots.address_id").
		Joins("JOIN addresses on votes.address_id = addresses.id").
		Joins("LEFT JOIN blocks on voter_delegation_snapshots.block_id=blocks.id").
		Group("votes.address_id, addresses.address").
		Order("MAX(blocks.height) ASC NULLS FIRST").
		Limit(n).
		Scan(&delegations).Error

	return delegations, err
}

type VoterDelegationsScanResult struct {
	Amount      string
	ValidatorID uint
	AddressID   uint
	Height      uint
}

func GetHighestBlockDelegationSnapshotsForAddresses(db *gorm.DB, addressIDs []uint) ([]VoterDelegationsScanResult, error) {

	var voterDelegations []VoterDelegationsScanResult

	maxAddressSubquery := db.Table("voter_delegation_snapshots snaps").
		Joins("JOIN blocks b on b.id = snaps.block_id").
		Group("snaps.address_id").
		Select("snaps.address_id, MAX(b.height) as max_height").
		Where("snaps.address_id IN (?)", addressIDs)

	snapIDSubquery := db.Table("voter_delegation_snapshots snaps").
		Select("snaps.id, snaps.address_id, b.height").
		Joins("JOIN blocks b ON b.id = snaps.block_id").
		Joins("JOIN (?) max_snaps ON max_snaps.address_id = snaps.address_id AND b.height = max_snaps.max_height", maxAddressSubquery)

	err := db.Table("voter_delegations vd").
		Joins("JOIN (?) snaps ON snaps.id = vd.voter_delegation_snapshot_id", snapIDSubquery).
		Select("vd.amount, vd.validator_id, snaps.address_id, snaps.height").Scan(&voterDelegations).Error

	return voterDelegations, err
}
