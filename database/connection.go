package database

import (
	"github.com/DefiantLabs/cosmos-indexer/cmd"
	"github.com/DefiantLabs/cosmos-indexer/config"
	dbTypes "github.com/DefiantLabs/cosmos-indexer/db"
	"gorm.io/gorm"
)

func ConnectToDBAndRunMigrations(config *config.Database, customModels []any) (*gorm.DB, error) {
	var err error
	db, err := cmd.ConnectToDBAndMigrate(*config)

	if err != nil {
		return nil, err
	}

	indexer := cmd.GetBuiltinIndexer()

	err = dbTypes.MigrateInterfaces(db, indexer.CustomModels)
	if err != nil {
		return nil, err
	}

	return db, nil
}
