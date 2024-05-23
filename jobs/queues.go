package jobs

import (
	"sync"
	"time"

	"github.com/DefiantLabs/cosmos-indexer/config"
	dbTypes "github.com/DefiantLabs/cosmos-indexer/db"
	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/DefiantLabs/probe/client"
	cosmosTypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	priorityProcessorLimit = 10

	priorityUpdateTimeDelta = time.Duration(2) * time.Hour

	priorityProcessorHaltOnError                = true
	priorityProcessorCheckDelegationsTableFirst = true
)

type PriorityAddressQueueData struct {
	AddressID uint
	Block     models.Block
}

func ProcessPriorityAddressesForDelegations(queue chan []PriorityAddressQueueData, callerWg *sync.WaitGroup, db *gorm.DB, conf *config.IndexConfig, client *client.ChainClient) {
	defer callerWg.Done()
	config.Log.Infof("Starting priority address processor")

	fatalErrorInWorker := false

	if db == nil {
		config.Log.Errorf("Priority address processor DB is nil")
		return
	}

	if conf == nil {
		config.Log.Errorf("Priority address processor IndexConfig is nil")
		return
	}

	workerSemaphore := make(chan struct{}, priorityProcessorLimit)
	wg := &sync.WaitGroup{}
	config.Log.Infof("Priority address processor watching for queue data")
	for v := range queue {
		if fatalErrorInWorker {
			config.Log.Fatalf("Fatal error in priority address queue worker, halting")
		}

		config.Log.Infof("Priority address processor received %d addresses", len(v))

		for _, queueData := range v {
			workerSemaphore <- struct{}{}
			wg.Add(1)

			// Skip delegations table check, this is a priority address and needs to be processed no matter the delegations table state
			// Also halt on fatal error, we want to stop processing if there is a fatal error in a worker since the indexed data will be inconsistent
			go ProcessDelegationsForDelegator(&workerSemaphore, wg, queueData.AddressID, &queueData.Block, db, conf, client, priorityProcessorCheckDelegationsTableFirst, priorityProcessorHaltOnError)
		}
	}

	// Final wait for all workers to finish before returning
	wg.Wait()

	config.Log.Infof("Finished processing priority addresses")
}

type VoterDelegationLastUpdatedSearch struct {
	ID          uint
	Address     string
	LastUpdated *time.Time
}

func ProcessDelegationsForDelegator(workerSemaphore *chan struct{}, wg *sync.WaitGroup, addressID uint, block *models.Block, db *gorm.DB, conf *config.IndexConfig, client *client.ChainClient, checkDelegationsTableFirst bool, haltOnFatalError bool) {
	if workerSemaphore != nil {
		defer func() { <-*workerSemaphore }()
	}

	if wg != nil {
		defer wg.Done()
	}

	if db == nil {
		config.Log.Errorf("ProcessDelegations worker DB is nil")
		if haltOnFatalError {
			config.Log.Fatal("Halting on fatal error in worker")
		}
		return
	}

	if conf == nil {
		config.Log.Errorf("ProcessDelegations worker IndexConfig is nil")
		if haltOnFatalError {
			config.Log.Fatal("Halting on fatal error in worker")
		}
		return
	}

	var needsUpdate bool = true
	var address string

	config.Log.Debugf("Processing delegations for address with ID %d", addressID)

	if checkDelegationsTableFirst {

		config.Log.Debugf("Checking delegations table for address with ID %d", addressID)

		firstVoterDelegation := VoterDelegationLastUpdatedSearch{}
		err := db.Table("addresses").
			Select("addresses.id as id, addresses.address as address, blocks.time_stamp as last_updated").
			Joins("LEFT JOIN voter_delegation_snapshots on voter_delegation_snapshots.address_id = addresses.id").
			Joins("LEFT JOIN blocks ON blocks.id=voter_delegation_snapshots.block_id").
			Where("addresses.id = ?", addressID).
			Order("blocks.height DESC").
			Limit(1).
			Scan(&firstVoterDelegation).Error

		if err != nil {
			config.Log.Errorf("ProcessDelegations worker failed to find address with ID %d. Err: %v", addressID, err)
			if haltOnFatalError {
				config.Log.Fatal("Halting on fatal error in worker")
			}
			return
		}

		if firstVoterDelegation.ID == 0 {
			config.Log.Errorf("ProcessDelegations worker did not find address with ID %d, cannot process", addressID)
			if haltOnFatalError {
				config.Log.Fatal("Halting on fatal error in worker")
			}
			return
		} else if firstVoterDelegation.LastUpdated == nil {
			config.Log.Infof("ProcessDelegations worker found address with ID %d but it has no delegation, processing", addressID)
			needsUpdate = true
			address = firstVoterDelegation.Address
		} else if time.Now().Add(-priorityUpdateTimeDelta).Before(*firstVoterDelegation.LastUpdated) {
			config.Log.Infof("ProcessDelegations worker found address with ID %d but it was updated recently", addressID)
			needsUpdate = false
			address = firstVoterDelegation.Address
		} else {
			config.Log.Infof("ProcessDelegations worker found address with ID %d and it needs updating", addressID)
			needsUpdate = true
			address = firstVoterDelegation.Address
		}
	} else {
		addressModel := models.Address{}

		err := db.Where("id = ?", addressID).Find(&addressModel).Error

		if err != nil {
			config.Log.Errorf("ProcessDelegations worker failed to find address with ID %d. Err: %v", addressID, err)
			if haltOnFatalError {
				config.Log.Fatal("Halting on fatal error in worker")
			}
			return
		}

		address = addressModel.Address
	}

	if needsUpdate && address != "" {
		var height *int64
		if block != nil {
			height = &block.Height
		}

		delegationResponses, err := requests.GetDelegations(address, height, *client)

		if err != nil {
			config.Log.Errorf("ProcessDelegations worker failed to get delegations for address with ID %d. Err: %v", addressID, err)
			if haltOnFatalError {
				config.Log.Fatal("Halting on fatal error in worker")
			}
			return
		}

		if len(delegationResponses) == 0 {
			config.Log.Infof("ProcessDelegations worker found no delegations for address with ID %d", addressID)
			return
		} else {
			config.Log.Infof("ProcessDelegations worker found %d delegations for address with ID %d", len(delegationResponses), addressID)

			err := db.Transaction(func(tx *gorm.DB) error {

				voterSnapshot := database.VoterDelegationSnapshot{
					BlockID:   block.ID,
					AddressID: addressID,
					UpdatedAt: time.Now(),
				}

				err := db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "block_id"}, {Name: "address_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"updated_at"}),
				}).Create(&voterSnapshot).Error

				if err != nil {
					return err
				}

				for _, delegation := range delegationResponses {

					if !delegation.Balance.Amount.IsZero() {
						err := InsertDelegationsForValidatorAddressAndDelegatorAddress(voterSnapshot.ID, addressID, delegation.Delegation.ValidatorAddress, delegation.Balance.Amount.String(), *block, conf, db)

						if err != nil {
							return err
						}
					}
				}

				return nil
			})

			if err != nil {
				config.Log.Errorf("ProcessDelegations worker failed to insert delegations for address with ID %d. Err: %v", addressID, err)
				if haltOnFatalError {
					config.Log.Fatal("Halting on fatal error in worker")
				}
			}
		}
	} else if address == "" {
		config.Log.Errorf("ProcessDelegations worker failed to find address string for address with ID %d", addressID)
		if haltOnFatalError {
			config.Log.Fatal("Halting on fatal error in worker")
		}
	}
}

// TODO: Optimize this? We could pass the entire array here and do a single transaction with reduced amount of queries
func InsertDelegationsForValidatorAddressAndDelegatorAddress(snapshotID uint, delegatorAddressID uint, validatorAddress string, amount string, block models.Block, conf *config.IndexConfig, db *gorm.DB) error {
	_, validatorAddressBytes, err := bech32.DecodeAndConvert(validatorAddress)

	if err != nil {
		config.Log.Errorf("Failed to decode validator address %s. Err: %v", validatorAddress, err)
		return err
	}

	validatorAccountAddress, err := cosmosTypes.Bech32ifyAddressBytes(conf.Probe.AccountPrefix, validatorAddressBytes)

	if err != nil {
		config.Log.Errorf("Failed to convert validator address %s to account address. Err: %v", validatorAddress, err)
		return err
	}

	valoperAddress, err := dbTypes.FindOrCreateAddressByAddress(db, validatorAddress)

	if err != nil {
		config.Log.Errorf("Failed to find or create validator address %s. Err: %v", validatorAddress, err)
		return err
	}

	validatorAccountAddressModel, err := dbTypes.FindOrCreateAddressByAddress(db, validatorAccountAddress)

	if err != nil {
		config.Log.Errorf("Failed to find or create validator account address %s. Err: %v", validatorAccountAddress, err)
		return err
	}

	validatorAddressModel, err := database.FindOrCreateValidatorByAddressPair(db, valoperAddress.ID, validatorAccountAddressModel.ID)

	if err != nil {
		config.Log.Errorf("Failed to find or create validator for valoper address %s and account address %s. Err: %v", valoperAddress.Address, validatorAccountAddressModel.Address, err)
		return err
	}

	delegation := database.VoterDelegation{
		ValidatorID:               validatorAddressModel.ID,
		Validator:                 validatorAddressModel,
		Amount:                    amount,
		VoterDelegationSnapshotID: snapshotID,
		VoterDelegationSnapshot:   database.VoterDelegationSnapshot{ID: snapshotID},
	}

	err = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "voter_delegation_snapshot_id"}, {Name: "validator_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"amount"}),
	}).Create(&delegation).Error

	if err != nil {
		config.Log.Errorf("Failed to create delegation for address with ID %d and validator address %s. Err: %v", delegatorAddressID, validatorAddress, err)
		return err
	}

	return nil

}
