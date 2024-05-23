package appconfig

import (
	"fmt"

	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/spf13/cobra"
)

type Task struct {
	Database               config.Database
	Probe                  config.Probe
	PowerUpdateTask        PowerUpdate            `mapstructure:"power-update"`
	DelegatorStakingUpdate DelegatorStakingUpdate `mapstructure:"delegator-staking-update"`
	ProposalStatusUpdate   ProposalStatusUpdate   `mapstructure:"proposal-status-update"`
	AverageBlockTime       uint64                 `mapstructure:"average-block-time"`
	LogLevel               string                 `mapstructure:"log-level"`
}

type validatorStakingTaskConfigs struct {
	Enabled                       bool    `mapstructure:"enabled"`
	Interval                      uint64  `mapstructure:"interval"`
	Cron                          string  `mapstructure:"cron"`
	DelegatorAmount               int     `mapstructure:"delegator-amount"`
	StartImmediately              bool    `mapstructure:"start-immediately"`
	Throttle                      int     `mapstructure:"throttle"`
	ConcurrentRequests            int     `mapstructure:"concurrent-requests"`
	UpdateHeight                  int64   `mapstructure:"update-height"`
	GetLastHeightInterval         bool    `mapstructure:"get-last-height-interval"`
	ConcurrentRequestFailureLimit float64 `mapstructure:"concurrent-request-failure-limit"`
}

type PowerUpdate struct {
	validatorStakingTaskConfigs
}

type DelegatorStakingUpdate struct {
	validatorStakingTaskConfigs
}

type ProposalStatusUpdate struct {
	Enabled            bool  `mapstructure:"enabled"`
	HeightToUpdateTo   int64 `mapstructure:"height-to-update-to"`
	HeightToUpdateFrom int64 `mapstructure:"height-to-update-from"`
	UpdateInterval     int64 `mapstructure:"update-interval"`
}

func SetupPowerUpdateTaskFlags(config *PowerUpdate, cmd *cobra.Command) {
	configName := "power-update"

	cmd.Flags().BoolVar(&config.Enabled, fmt.Sprintf("%s.enabled", configName), false, "Enable power update task")
	cmd.Flags().Uint64Var(&config.Interval, fmt.Sprintf("%s.interval", configName), 240, "Interval in minutes at which to update power")
	cmd.Flags().StringVar(&config.Cron, fmt.Sprintf("%s.cron", configName), "", "Cron expression at which to update power, will override interval if set")
	cmd.Flags().IntVar(&config.DelegatorAmount, fmt.Sprintf("%s.delegator-amount", configName), 500, "Amount of delegators to update delegations for before calculating the whole voter power base (max of 500)")
	cmd.Flags().BoolVar(&config.StartImmediately, fmt.Sprintf("%s.start-immediately", configName), false, "Start the power update task immediately")
	cmd.Flags().IntVar(&config.Throttle, fmt.Sprintf("%s.throttle", configName), 1000, "Amount in milliseconds to throttle when making RPC query requests chunks")
	cmd.Flags().IntVar(&config.ConcurrentRequests, fmt.Sprintf("%s.concurrent-requests", configName), 1, "Amount of concurrent requests to make to the RPC")
	cmd.Flags().Int64Var(&config.UpdateHeight, fmt.Sprintf("%s.update-height", configName), 0, "Height at which to update power. Choosing old heights may run into pruning issues.")
	cmd.Flags().BoolVar(&config.GetLastHeightInterval, fmt.Sprintf("%s.get-last-height-interval", configName), false, "Get the last height and use this to determine what height to use when executing the power update task.")
	cmd.Flags().Float64Var(&config.ConcurrentRequestFailureLimit, fmt.Sprintf("%s.concurrent-request-failure-limit", configName), 0.5, "The percentage of failures allowed before the task will stop processing during request chunks.")
}

func SetupDelegatorUpdateTaskFlags(config *DelegatorStakingUpdate, cmd *cobra.Command) {
	configName := "delegations-update"

	cmd.Flags().BoolVar(&config.Enabled, fmt.Sprintf("%s.enabled", configName), false, "Enable delegator staking update task")
	cmd.Flags().Uint64Var(&config.Interval, fmt.Sprintf("%s.interval", configName), 60, "Interval at which to update delegator staking")
	cmd.Flags().StringVar(&config.Cron, fmt.Sprintf("%s.cron", configName), "", "Cron expression at which to update power, will override interval if set")
	cmd.Flags().IntVar(&config.DelegatorAmount, fmt.Sprintf("%s.delegator-amount", configName), 1000, "Amount of delegators to update staking for")
	cmd.Flags().BoolVar(&config.StartImmediately, fmt.Sprintf("%s.start-immediately", configName), false, "Start the delegator staking update immediately")
	cmd.Flags().IntVar(&config.Throttle, fmt.Sprintf("%s.throttle", configName), 500, "Amount in milliseconds to throttle when making RPC query requests chunks")
	cmd.Flags().IntVar(&config.ConcurrentRequests, fmt.Sprintf("%s.concurrent-requests", configName), 1, "Amount of concurrent requests to make to the RPC")
	cmd.Flags().Int64Var(&config.UpdateHeight, fmt.Sprintf("%s.update-height", configName), 0, "Height at which to update power. Choosing old heights may run into pruning issues.")
	cmd.Flags().BoolVar(&config.GetLastHeightInterval, fmt.Sprintf("%s.get-last-height-interval", configName), false, "Get the last height and use this to determine what height to use when executing the power update task.")
	cmd.Flags().Float64Var(&config.ConcurrentRequestFailureLimit, fmt.Sprintf("%s.concurrent-request-failure-limit", configName), 0.5, "The percentage of failures allowed before the task will stop processing during request chunks.")
}

func SetupProposalStatusUpdateTaskFlags(config *ProposalStatusUpdate, cmd *cobra.Command) {
	configName := "proposal-status-update"

	cmd.Flags().BoolVar(&config.Enabled, fmt.Sprintf("%s.enabled", configName), false, "Enable proposal status update task")
	cmd.Flags().Int64Var(&config.HeightToUpdateTo, fmt.Sprintf("%s.height-to-update-to", configName), 0, "Height to update proposal status to")
	cmd.Flags().Int64Var(&config.HeightToUpdateFrom, fmt.Sprintf("%s.height-to-update-from", configName), 0, "Height to update proposal status from")
	cmd.Flags().Int64Var(&config.UpdateInterval, fmt.Sprintf("%s.update-interval", configName), 100, "Interval at which to update proposal status")
}
