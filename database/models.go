package database

import (
	"time"

	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/shopspring/decimal"
)

func GetCustomModels() []any {
	return []any{
		&Vote{},
		&Proposal{},
		&VoterDelegationSnapshot{},
		&VoterDelegation{},
		&ValidatorAmountStaked{},
		&Validator{},
		&JobStatus{},
		&ProposalStatus{},
	}
}

// These are the indexer's custom models
// They are used to store the parsed data in the database
type VoteOption int64

const (
	Empty VoteOption = iota
	Yes
	Abstain
	No
	Veto
)

var VoteOptionStrings = map[VoteOption]string{
	Empty:   "Empty",
	Yes:     "Yes",
	Abstain: "Abstain",
	No:      "No",
	Veto:    "Veto",
}

func (v VoteOption) String() string {
	return VoteOptionStrings[v]
}

// Vote is a custom model that stores the parsed vote data in the database.
type Vote struct {
	ID         uint
	Tx         models.Tx
	TxID       uint
	Block      models.Block
	BlockID    uint `gorm:"index"`
	AddressID  uint `gorm:"index:idx_address_proposal,priority:2,unique"`
	Address    models.Address
	Option     VoteOption
	Proposal   Proposal
	ProposalID uint64 `gorm:"index:idx_address_proposal,priority:1,unique"`
}

type Proposal struct {
	ID         uint64
	ProposalID uint64 `gorm:"unique"`
	Votes      []Vote
}

// This junction table is used to store the snapshot of the voter's delegation at a specific block
// It is necessary in the case that a snapshot finds no delegations for a voter, we need to record this so we can set their
// voting power to 0 during that time window.
type VoterDelegationSnapshot struct {
	ID        uint
	AddressID uint `gorm:"index:idx_block_address,priority:2,unique;index:idx_address"`
	Address   models.Address
	BlockID   uint `gorm:"index:idx_block_address,priority:1,unique"`
	Block     models.Block
	UpdatedAt time.Time
}

type VoterDelegation struct {
	ID                        uint
	ValidatorID               uint `gorm:"index:validator_snapshot,priority:2,unique"`
	Validator                 Validator
	Amount                    string
	VoterDelegationSnapshotID uint `gorm:"index:validator_snapshot,priority:1,unique"`
	VoterDelegationSnapshot   VoterDelegationSnapshot
}

// Validator staked amounts at a specific block. No need for a snapshot table since there will only ever be a single entry.
type ValidatorAmountStaked struct {
	ID          uint
	ValidatorID uint `gorm:"index:idx_block_validator,priority:2,unique;"`
	Validator   Validator
	Amount      string
	BlockID     uint `gorm:"index:idx_block_validator,priority:1,unique"`
	Block       models.Block
}

type Validator struct {
	ID               uint
	ValoperAddressID uint `gorm:"index"`
	ValoperAddress   models.Address
	AccountAddressID uint `gorm:"index"`
	AccountAddress   models.Address
}

type Migration struct {
	ID          uint
	MigrationID string `gorm:"unique"`
}

type Job uint

const (
	JobPowerUpdate Job = iota
	JobUpdateDelegations
)

type JobStatus struct {
	ID        uint
	JobID     Job `gorm:"index"`
	BlockID   *uint
	Error     string
	StartTime *time.Time
	EndTime   *time.Time
}

type ProposalStatus struct {
	ID            uint
	ProposalID    uint `gorm:"index:idx_proposal_block,priority:1,unique"`
	Proposal      Proposal
	BlockID       uint `gorm:"index:idx_proposal_block,priority:2,unique"`
	Block         models.Block
	YesAmount     decimal.Decimal `gorm:"type:decimal(78,0);"`
	NoAmount      decimal.Decimal `gorm:"type:decimal(78,0);"`
	EmptyAmount   decimal.Decimal `gorm:"type:decimal(78,0);"`
	AbstainAmount decimal.Decimal `gorm:"type:decimal(78,0);"`
	VetoAmount    decimal.Decimal `gorm:"type:decimal(78,0);"`
}
