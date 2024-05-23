package database

import (
	"time"

	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"gorm.io/gorm"
)

func CreateJobStatus(db *gorm.DB, blockID *uint, job Job, startTime *time.Time) (*JobStatus, error) {
	status := JobStatus{
		JobID: job,
	}

	if startTime == nil {
		t := time.Now()
		startTime = &t
	}

	status.StartTime = startTime

	if blockID != nil {
		status.BlockID = blockID
	}

	err := db.Create(&status).Error

	return &status, err
}

func GetBlockHeightForHighestJob(db *gorm.DB, job Job) (*models.Block, error) {
	var block models.Block
	err := db.Table("job_statuses").
		Select("blocks.*").
		Where("job_id = ?", job).
		Joins("JOIN blocks ON job_statuses.block_id = blocks.id").
		Order("blocks.height DESC").
		First(&block).Error

	return &block, err
}

func UpdateJobStatusError(db *gorm.DB, jobStatusID uint, err error) error {
	now := time.Now()

	return db.Model(&JobStatus{}).
		Where("id = ?", jobStatusID).
		Updates(JobStatus{
			Error:   err.Error(),
			EndTime: &now,
		}).Error
}

func UpdateJobStatusEndTime(db *gorm.DB, jobStatusID uint) error {
	now := time.Now()

	return db.Model(&JobStatus{}).
		Where("id = ?", jobStatusID).
		Updates(JobStatus{
			EndTime: &now,
		}).Error
}
