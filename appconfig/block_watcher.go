package appconfig

import (
	"fmt"

	"github.com/spf13/cobra"
)

type BlockWatcher struct {
	StakingUpdateBlockThreshold     int64   `mapstructure:"staking-update-block-threshold"`
	StakingUpdateDelegatorLimit     int     `mapstructure:"staking-update-delegator-limit"`
	StakingUpdateConcurrentRequests int     `mapstructure:"staking-update-concurrent-requests"`
	StakingUpdateErrorLimit         float64 `mapstructure:"staking-update-error-limit"`
	StakingUpdateBlockThrottle      int     `mapstructure:"staking-update-block-throttle"`

	ProposalStatusUpdateThreshold int64 `mapstructure:"proposal-status-update-threshold"`
	ProposalStatusUpdateInterval  int64 `mapstructure:"proposal-status-update-interval"`
}

func SetupBlockWatcherFlags(config *BlockWatcher, cmd *cobra.Command) {
	configName := "block-watcher"

	cmd.PersistentFlags().Int64Var(&config.StakingUpdateBlockThreshold, fmt.Sprintf("%s.staking-update-block-threshold", configName), 200, "The block threshold at which to update staking information")
	cmd.PersistentFlags().IntVar(&config.StakingUpdateConcurrentRequests, fmt.Sprintf("%s.staking-update-concurrent-requests", configName), 10, "The amount of concurrent requests to make to the RPC")
	cmd.PersistentFlags().IntVar(&config.StakingUpdateDelegatorLimit, fmt.Sprintf("%s.staking-update-delegator-limit", configName), 100, "The amount of delegators to update staking information for")
	cmd.PersistentFlags().Float64Var(&config.StakingUpdateErrorLimit, fmt.Sprintf("%s.staking-update-error-limit", configName), 0.5, "The percentage of failures allowed before the task will stop processing during request chunks")
	cmd.PersistentFlags().IntVar(&config.StakingUpdateBlockThrottle, fmt.Sprintf("%s.staking-update-block-throttle", configName), 2000, "The amount in milliseconds to throttle when making RPC query requests chunks")

	cmd.PersistentFlags().Int64Var(&config.ProposalStatusUpdateThreshold, fmt.Sprintf("%s.proposal-status-update-threshold", configName), 1000, "The block threshold at which to run updates to proposal status")
	cmd.PersistentFlags().Int64Var(&config.ProposalStatusUpdateInterval, fmt.Sprintf("%s.proposal-status-update-interval", configName), 500, "The interval between blocks in which to calculate proposal status")
}
