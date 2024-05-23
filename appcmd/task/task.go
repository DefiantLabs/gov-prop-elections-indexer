package task

import (
	"encoding/json"
	"os"
	"sort"

	"github.com/DefiantLabs/cosmos-indexer/cmd"
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/cosmos-indexer/db/models"
	"github.com/DefiantLabs/cosmos-indexer/probe"
	"github.com/DefiantLabs/cosmos-indexer/rpc"
	"github.com/DefiantLabs/gov-prop-elections-indexer/appconfig"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/jobs"
	"github.com/DefiantLabs/gov-prop-elections-indexer/utils"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	_ "github.com/go-co-op/gocron"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

var taskConfig appconfig.Task

const (
	PowerUpdateDefaultInterval = 240
)

func init() {
	sharedConfigs := []*cobra.Command{
		TaskCmd, powerUpdateExecutorCmd, proposalStatusUpdateExecutorCmd, proposalStatusCheckerExecutorCmd,
	}
	for _, cmd := range sharedConfigs {
		config.SetupDatabaseFlags(&taskConfig.Database, cmd)
		config.SetupProbeFlags(&taskConfig.Probe, cmd)
	}

	appconfig.SetupPowerUpdateTaskFlags(&taskConfig.PowerUpdateTask, TaskCmd)
	appconfig.SetupProposalStatusUpdateTaskFlags(&taskConfig.ProposalStatusUpdate, TaskCmd)

	appconfig.SetupPowerUpdateTaskFlags(&taskConfig.PowerUpdateTask, powerUpdateExecutorCmd)
	appconfig.SetupProposalStatusUpdateTaskFlags(&taskConfig.ProposalStatusUpdate, proposalStatusUpdateExecutorCmd)
	appconfig.SetupProposalStatusUpdateTaskFlags(&taskConfig.ProposalStatusUpdate, proposalStatusCheckerExecutorCmd)

	TaskCmd.Flags().Uint64Var(&taskConfig.AverageBlockTime, "average-block-time", 6, "Average block time in seconds. Used when running in certain task configs.")

	TaskCmd.AddCommand(powerUpdateExecutorCmd)
	TaskCmd.AddCommand(proposalStatusUpdateExecutorCmd)
	TaskCmd.AddCommand(proposalStatusCheckerExecutorCmd)
}

var TaskCmd = &cobra.Command{
	Use:               "task",
	Short:             "Run various tasks immediately or on schedules",
	PersistentPreRunE: setupTask,
	Run:               task,
}

var taskSetupDB *gorm.DB

func setupTask(command *cobra.Command, args []string) error {
	cmd.BindFlags(command, cmd.GetViperConfig())

	var err error
	indexer := cmd.GetBuiltinIndexer()
	taskSetupDB, err = database.ConnectToDBAndRunMigrations(&taskConfig.Database, indexer.CustomModels)

	if err != nil {
		config.Log.Fatal("Failed to connect to database and run migrations", err)
	}

	config.DoConfigureLogger("./task.log", taskConfig.LogLevel, true)

	return nil
}

func task(cmd *cobra.Command, args []string) {
	config.Log.Infof("Starting task scheduler")
}

var powerUpdateExecutorCmd = &cobra.Command{
	Use:   "update-power",
	Short: "Run the voter power update task and exit after running",
	Long: `This task will retrieve all currently bonded validators and get their current staked amount. It will also run
	the delegator update task to update the delegations for the oldest delegators in the database. It will then calculate the total voting power
	and use this as a snapshot for the database voting power value.`,
	RunE: powerUpdateExecutor,
}

func commitJobStatusError(jobID uint, err error) error {
	err1 := database.UpdateJobStatusError(taskSetupDB, jobID, err)

	config.Log.Errorf("Job with ID %d errored out: %v", jobID, err)
	if err1 != nil {
		config.Log.Errorf("Failed to update job status with error while attempting to error out: %v", err1)
		return err
	}

	config.Log.Errorf("Job with ID %d errored out: %v", jobID, err)

	return err
}

func powerUpdateExecutor(cmd *cobra.Command, args []string) error {

	probeClient := probe.GetProbeClient(taskConfig.Probe, nil)

	config.Log.Infof("Running power update task")

	job, err := database.CreateJobStatus(taskSetupDB, nil, database.JobPowerUpdate, nil)

	if err != nil {
		return commitJobStatusError(job.ID, err)
	}

	powerUpdateConfig := taskConfig.PowerUpdateTask

	var runtimeHeight int64
	var block *models.Block
	var blockRPC *coretypes.ResultBlock

	if powerUpdateConfig.GetLastHeightInterval {
		config.Log.Infof("Getting last height for power update task")

		block, err = database.GetBlockHeightForHighestJob(taskSetupDB, database.JobPowerUpdate)

		if err != nil && err != gorm.ErrRecordNotFound {
			return commitJobStatusError(job.ID, err)
		} else if err == gorm.ErrRecordNotFound {
			config.Log.Infof("No previous power update job found, running for latest height")

			_, block, err = database.FindLatestCommittedHeightAndFindOrCreate(probeClient, taskSetupDB, taskConfig.Probe.ChainID)
			if err != nil {
				return commitJobStatusError(job.ID, err)
			}
		} else {
			heightToCheck := block.Height + (int64(taskConfig.AverageBlockTime) * PowerUpdateDefaultInterval)
			var blockRPC *coretypes.ResultBlock
			attempts := 0
			for {
				blockRPC, err = rpc.GetBlock(probeClient, heightToCheck)
				if err != nil && attempts < 5 && heightToCheck > (block.Height+1) {
					heightToCheck /= 2
				} else {
					break
				}
			}

			if err != nil {
				return commitJobStatusError(job.ID, err)
			}

			block, err = database.FindOrCreateBlockByHeightAndChainID(taskSetupDB, *blockRPC, taskConfig.Probe.ChainID)

			if err != nil {
				return commitJobStatusError(job.ID, err)
			}
		}

		runtimeHeight = block.Height
	} else if powerUpdateConfig.UpdateHeight != 0 {
		config.Log.Infof("Using explicit update height")

		blockRPC, err = rpc.GetBlock(probeClient, powerUpdateConfig.UpdateHeight)
		if err != nil {
			return commitJobStatusError(job.ID, err)
		}

		block, err = database.FindOrCreateBlockByHeightAndChainID(taskSetupDB, *blockRPC, taskConfig.Probe.ChainID)
		if err != nil {
			return commitJobStatusError(job.ID, err)
		}

		runtimeHeight = block.Height
	} else {
		config.Log.Infof("Getting latest height for power update task")
		_, block, err = database.FindLatestCommittedHeightAndFindOrCreate(probeClient, taskSetupDB, taskConfig.Probe.ChainID)
		if err != nil {
			return commitJobStatusError(job.ID, err)
		}

		runtimeHeight = block.Height
	}

	config.Log.Infof("Running for height %d", runtimeHeight)

	err = jobs.UpdateDelegatorAndValidatorPowerAtBlock(
		&powerUpdateConfig.DelegatorAmount,
		powerUpdateConfig.ConcurrentRequests,
		powerUpdateConfig.Throttle,
		powerUpdateConfig.ConcurrentRequestFailureLimit,
		block,
		taskSetupDB,
		probeClient,
		&taskConfig.Probe,
	)

	if err != nil {
		return commitJobStatusError(job.ID, err)
	}

	err = database.UpdateJobStatusEndTime(taskSetupDB, job.ID)

	if err != nil {
		return commitJobStatusError(job.ID, err)
	}

	config.Log.Infof("Power update task completed successfully")

	return nil
}

var proposalStatusUpdateExecutorCmd = &cobra.Command{
	Use:   "update-proposal-status",
	Short: "Run the proposal status update task and exit after running",
	Long: `This task will retrieve all currently bonded validators and get their current staked amount. It will also run
	the delegator update task to update the delegations for the oldest delegators in the database. It will then calculate the total voting power
	and use this as a snapshot for the database proposal status value. It will check all heights from the last proposal status update to this height and cache all results`,
	RunE: proposalStatusStakingTrackerExecutor,
}

func proposalStatusStakingTrackerExecutor(cmd *cobra.Command, args []string) error {
	config.Log.Infof("Running proposal status update task")

	// Get the first indexed vote
	voteHeights, err := database.GetEarliestVoteForAllProposals(taskSetupDB)

	if err != nil {
		config.Log.Errorf("Failed to get earliest vote for all proposals: %v", err)
		return err
	} else if len(voteHeights) == 0 {
		config.Log.Infof("No votes found, skipping proposal status update task")
		return nil
	}

	if taskConfig.ProposalStatusUpdate.HeightToUpdateTo == 0 {
		config.Log.Infof("No height to update to, getting highest tx indexed block")
		// get heighest tx_indexed block
		block, err := database.GetHighestTXIndexedBlock(taskSetupDB)

		if err != nil {
			config.Log.Errorf("Failed to get highest tx indexed block: %v", err)
			return err
		}

		taskConfig.ProposalStatusUpdate.HeightToUpdateTo = block.Height
		config.Log.Infof("Setting height to update to %d", taskConfig.ProposalStatusUpdate.HeightToUpdateTo)
	} else {
		config.Log.Infof("Using height to update to %d", taskConfig.ProposalStatusUpdate.HeightToUpdateTo)
	}

	var proposalIds []uint

	for _, voteHeight := range voteHeights {
		proposalIds = append(proposalIds, voteHeight.ProposalID)
	}

	config.Log.Infof("Found %d proposals to update (%v)", len(proposalIds), proposalIds)

	config.Log.Infof("Gathering previous proposal status to see where updates need to happen")

	proposalRequiredStatusHeightIntervals := make(map[uint][]int64)

	// build up an interval of heights that MUST exist in the statuses array for each entry from earliest vote height -> update height
	for _, voteStartHeight := range voteHeights {
		config.Log.Infof("Building interval for proposal %d, proposal height %d, height to update to %d, and interval %d", voteStartHeight.ProposalID, voteStartHeight.Height, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, taskConfig.ProposalStatusUpdate.UpdateInterval)
		intervalList := utils.BuildIntervalFromStartAndEndRoundedToInterval(voteStartHeight.Height, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, taskConfig.ProposalStatusUpdate.UpdateInterval)

		proposalRequiredStatusHeightIntervals[voteStartHeight.ProposalID] = intervalList

		config.Log.Infof("Built interval with len %d", len(intervalList))
	}

	previousProposalStatuses, err := database.GetProposalStatusesBeforeHeightByProposalID(taskSetupDB, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, proposalIds)

	if err != nil {
		config.Log.Errorf("Failed to get previous proposal statuses: %v", err)
		return err
	}

	mappedPreviousProposalStatusHeights := make(map[uint]map[int64]struct{})

	for _, previousProposalStatus := range previousProposalStatuses {
		if _, ok := mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID]; !ok {
			mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID] = make(map[int64]struct{})
		}
		mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID][previousProposalStatus.Height] = struct{}{}
	}

	config.Log.Infof("Found %d previous proposal statuses, checking for gaps", len(previousProposalStatuses))

	proposalStatusMissingHeights := make(map[uint][]int64)

	for _, voteStartHeight := range voteHeights {
		proposalStatusMissingHeights[voteStartHeight.ProposalID] = []int64{}

		for _, intervalVal := range proposalRequiredStatusHeightIntervals[voteStartHeight.ProposalID] {
			if _, ok := mappedPreviousProposalStatusHeights[voteStartHeight.ProposalID][intervalVal]; !ok {
				proposalStatusMissingHeights[voteStartHeight.ProposalID] = append(proposalStatusMissingHeights[voteStartHeight.ProposalID], intervalVal)
			}
		}
	}

	realProposalMissingHeights := make(map[uint][]int64)
	for proposalID, proposalMissingHeights := range proposalStatusMissingHeights {
		if len(proposalStatusMissingHeights[proposalID]) > 0 {
			config.Log.Infof("Found missing heights for proposal %d: %v", len(proposalMissingHeights), proposalMissingHeights)
			realProposalMissingHeights[proposalID] = proposalMissingHeights
		} else {
			config.Log.Infof("No missing heights for proposal %d", proposalMissingHeights)
		}
	}

	config.Log.Infof("Found %d proposals missing status height, updating", len(realProposalMissingHeights))

	var finalProposalIDs []uint
	for proposalID := range realProposalMissingHeights {
		finalProposalIDs = append(finalProposalIDs, proposalID)
	}

	sort.Slice(finalProposalIDs, func(i, j int) bool {
		return finalProposalIDs[i] < finalProposalIDs[j]
	})

	for _, proposalID := range finalProposalIDs {

		missingHeights := realProposalMissingHeights[proposalID]

		missingBlocksModels, err := database.GetBlocksByHeights(taskSetupDB, missingHeights, taskConfig.Probe.ChainID)

		if err != nil {
			config.Log.Errorf("Failed to get blocks for missing heights: %v", err)
			return err
		}

		err = jobs.UpdateProposalStatusForHeights(
			taskSetupDB,
			missingBlocksModels,
			proposalID,
		)

		if err != nil {
			config.Log.Errorf("Failed to update proposal statuses for proposal %d at missing heights: %v", proposalID, err)
			return err
		}
	}

	return nil
}

var proposalStatusCheckerExecutorCmd = &cobra.Command{
	Use:   "check-proposal-status",
	Short: "Run the proposal status checker task and exit after running",
	Long:  `This task will check to ensure proposal status values exist for every block at a set interval, it will report any missing intervals.`,
	RunE:  proposalStatusCheckerExecutor,
}

func proposalStatusCheckerExecutor(cmd *cobra.Command, args []string) error {
	config.Log.Infof("Running proposal status checker task")

	// Get the first indexed vote
	voteHeights, err := database.GetEarliestVoteForAllProposals(taskSetupDB)

	if err != nil {
		config.Log.Errorf("Failed to get earliest vote for all proposals: %v", err)
		return err
	} else if len(voteHeights) == 0 {
		config.Log.Infof("No votes found, skipping proposal status update task")
		return nil
	}

	if taskConfig.ProposalStatusUpdate.HeightToUpdateTo == 0 {
		config.Log.Infof("No height to update to, getting highest tx indexed block")
		// get heighest tx_indexed block
		block, err := database.GetHighestTXIndexedBlock(taskSetupDB)

		if err != nil {
			config.Log.Errorf("Failed to get highest tx indexed block: %v", err)
			return err
		}

		taskConfig.ProposalStatusUpdate.HeightToUpdateTo = block.Height
		config.Log.Infof("Setting height to update to %d", taskConfig.ProposalStatusUpdate.HeightToUpdateTo)
	} else {
		config.Log.Infof("Using height to update to %d", taskConfig.ProposalStatusUpdate.HeightToUpdateTo)
	}

	var proposalIds []uint

	for _, voteHeight := range voteHeights {
		proposalIds = append(proposalIds, voteHeight.ProposalID)
	}

	config.Log.Infof("Found %d proposals to update (%v)", len(proposalIds), proposalIds)

	config.Log.Infof("Gathering previous proposal status to see where updates need to happen")

	proposalRequiredStatusHeightIntervals := make(map[uint][]int64)

	// build up an interval of heights that MUST exist in the statuses array for each entry from earliest vote height -> update height
	for _, voteStartHeight := range voteHeights {
		config.Log.Infof("Building interval for proposal %d, proposal height %d, height to update to %d, and interval %d", voteStartHeight.ProposalID, voteStartHeight.Height, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, taskConfig.ProposalStatusUpdate.UpdateInterval)
		intervalList := utils.BuildIntervalFromStartAndEndRoundedToInterval(voteStartHeight.Height, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, taskConfig.ProposalStatusUpdate.UpdateInterval)

		proposalRequiredStatusHeightIntervals[voteStartHeight.ProposalID] = intervalList

		config.Log.Infof("Built interval with len %d", len(intervalList))
	}

	previousProposalStatuses, err := database.GetProposalStatusesBeforeHeightByProposalID(taskSetupDB, taskConfig.ProposalStatusUpdate.HeightToUpdateTo, proposalIds)

	if err != nil {
		config.Log.Errorf("Failed to get previous proposal statuses: %v", err)
		return err
	}

	mappedPreviousProposalStatusHeights := make(map[uint]map[int64]struct{})

	for _, previousProposalStatus := range previousProposalStatuses {
		if _, ok := mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID]; !ok {
			mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID] = make(map[int64]struct{})
		}
		mappedPreviousProposalStatusHeights[previousProposalStatus.ProposalID][previousProposalStatus.Height] = struct{}{}
	}

	config.Log.Infof("Found %d previous proposal statuses, checking for gaps", len(previousProposalStatuses))

	proposalStatusMissingHeights := make(map[uint][]int64)

	for _, voteStartHeight := range voteHeights {
		proposalStatusMissingHeights[voteStartHeight.ProposalID] = []int64{}

		for _, intervalVal := range proposalRequiredStatusHeightIntervals[voteStartHeight.ProposalID] {
			if _, ok := mappedPreviousProposalStatusHeights[voteStartHeight.ProposalID][intervalVal]; !ok {
				proposalStatusMissingHeights[voteStartHeight.ProposalID] = append(proposalStatusMissingHeights[voteStartHeight.ProposalID], intervalVal)
			}
		}
	}

	realProposalMissingHeights := make(map[uint][]int64)
	for proposalID, proposalMissingHeights := range proposalStatusMissingHeights {
		if len(proposalStatusMissingHeights[proposalID]) > 0 {
			config.Log.Infof("Found missing heights for proposal %d: %v", proposalID, len(proposalMissingHeights))
			realProposalMissingHeights[proposalID] = proposalMissingHeights
		} else {
			config.Log.Infof("No missing heights for proposal %d", proposalMissingHeights)
		}
	}

	config.Log.Infof("Found %d proposals missing status height, dumping results", len(realProposalMissingHeights))

	// write out the missing heights to a file for further analysis
	f, _ := os.Create("proposal-status-missing-heights.json")
	defer f.Close()
	jsonDump, _ := json.MarshalIndent(realProposalMissingHeights, "", "\t")
	f.Write(jsonDump)

	return nil
}
