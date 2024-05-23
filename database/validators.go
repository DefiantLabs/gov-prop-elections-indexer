package database

import (
	"errors"
	"time"

	dbTypes "github.com/DefiantLabs/cosmos-indexer/db"
	cosmosTypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"gorm.io/gorm"
)

func FindOrCreateValidatorByAddressPair(db *gorm.DB, valoperAddressID uint, accountAddressID uint) (Validator, error) {
	if valoperAddressID == 0 {
		return Validator{}, errors.New("valoper address is 0")
	}

	if accountAddressID == 0 {
		return Validator{}, errors.New("account address is 0")
	}

	addr := Validator{
		ValoperAddressID: valoperAddressID,
		AccountAddressID: accountAddressID,
	}
	err := db.Where(&addr).FirstOrCreate(&addr).Error
	return addr, err
}

func TranslateAndFindOrCreateValidatorByAccountAddress(db *gorm.DB, operatorAddress string, accountPrefix string) (Validator, error) {
	var validator Validator

	_, validatorAddressBytes, err := bech32.DecodeAndConvert(operatorAddress)
	if err != nil {
		return validator, err
	}

	validatorAccountAddress, err := cosmosTypes.Bech32ifyAddressBytes(accountPrefix, validatorAddressBytes)
	if err != nil {
		return validator, err
	}

	valoperAddress, err := dbTypes.FindOrCreateAddressByAddress(db, operatorAddress)

	if err != nil {
		return validator, err
	}

	validatorAccountAddressModel, err := dbTypes.FindOrCreateAddressByAddress(db, validatorAccountAddress)

	if err != nil {
		return validator, err
	}

	validator, err = FindOrCreateValidatorByAddressPair(db, valoperAddress.ID, validatorAccountAddressModel.ID)

	if err != nil {
		return validator, err
	}

	return validator, nil
}

type validatorAmountStakedScanResult struct {
	Amount      string
	ValidatorID uint
	Height      uint
	TimeStamp   time.Time
}

func GetHighestBlockAmountStakedForValidators(db *gorm.DB, validatorIDs []uint) ([]validatorAmountStakedScanResult, error) {
	var validatorAmountStakeds []validatorAmountStakedScanResult

	err := heighestStakedAmountsQueryBuilder(db, validatorIDs).Scan(&validatorAmountStakeds).Error

	return validatorAmountStakeds, err
}

func heighestStakedAmountsQueryBuilder(db *gorm.DB, validatorIDs []uint) *gorm.DB {
	maxValidatorStakedAmountSubquery := db.Table("validator_amount_stakeds vsa").
		Joins("JOIN blocks b on b.id = vsa.block_id").
		Group("vsa.validator_id").
		Select("vsa.validator_id, MAX(b.height) as max_height")

	if len(validatorIDs) > 0 {
		maxValidatorStakedAmountSubquery = maxValidatorStakedAmountSubquery.Where("vsa.validator_id IN (?)", validatorIDs)
	}

	return db.Table("validator_amount_stakeds vsa").
		Joins("JOIN blocks b on b.id = vsa.block_id").
		Joins("JOIN (?) max_vsas ON max_vsas.validator_id = vsa.validator_id AND b.height = max_vsas.max_height", maxValidatorStakedAmountSubquery).
		Select("vsa.amount, vsa.validator_id, b.height, b.time_stamp")
}
