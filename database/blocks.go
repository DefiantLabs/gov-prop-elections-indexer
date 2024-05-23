package database

import (
	"github.com/DefiantLabs/cosmos-indexer/core"
	dbTypes "github.com/DefiantLabs/cosmos-indexer/db"
	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/DefiantLabs/probe/client"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"gorm.io/gorm"
)

func FindLatestCommittedHeightAndFindOrCreate(probeClient *client.ChainClient, db *gorm.DB, chainID string) (*coretypes.ResultBlock, *models.Block, error) {
	blockRPC, err := requests.GetLatestCommittedHeight(probeClient)

	if err != nil {
		return nil, nil, err
	}

	block, err := FindOrCreateBlockByHeightAndChainID(db, *blockRPC, chainID)

	if err != nil {
		return nil, nil, err
	}

	return blockRPC, block, nil
}

func FindOrCreateBlockByHeightAndChainID(db *gorm.DB, blockRPC coretypes.ResultBlock, chainID string) (*models.Block, error) {

	chain := models.Chain{
		ChainID: chainID,
	}

	chainDBID, err := dbTypes.GetDBChainID(db, chain)
	if err != nil {
		return nil, err
	}

	block, err := core.ProcessBlock(&blockRPC, nil, chainDBID)

	if err != nil {
		return nil, err
	}

	prop, err := dbTypes.FindOrCreateAddressByAddress(db, block.ProposerConsAddress.Address)

	if err != nil {
		return nil, err
	}

	blockCreateIfNotExists := models.Block{
		Height:                blockRPC.Block.Height,
		ChainID:               chainDBID,
		TimeStamp:             blockRPC.Block.Time,
		ProposerConsAddressID: prop.ID,
	}

	err = db.Where(models.Block{Height: blockRPC.Block.Height, ChainID: chainDBID}).
		First(&block).Error

	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	} else if err == gorm.ErrRecordNotFound {
		err = db.Create(&blockCreateIfNotExists).Error
		if err != nil {
			return nil, err
		}

		return &blockCreateIfNotExists, nil
	}

	return &block, err
}

func GetBlocksByHeights(db *gorm.DB, heights []int64, chainID string) ([]models.Block, error) {
	var blocks []models.Block

	chain := models.Chain{
		ChainID: chainID,
	}

	chainDBID, err := dbTypes.GetDBChainID(db, chain)
	if err != nil {
		return nil, err
	}

	err = db.Table("blocks").
		Select("blocks.*").
		Joins("JOIN chains on blocks.chain_id = chains.id").
		Where("blocks.height IN (?) AND chains.id = ? AND blocks.tx_indexed=true", heights, chainDBID).
		Order("blocks.height ASC").
		Find(&blocks).Error

	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func GetHighestTXIndexedBlock(db *gorm.DB) (*models.Block, error) {
	var block models.Block

	err := db.Table("blocks").
		Select("blocks.height").
		Where("tx_indexed = true").
		Order("blocks.height DESC").
		First(&block).Error

	if err != nil {
		return nil, err
	}
	return &block, nil
}
