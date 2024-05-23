package appconfig

import "github.com/spf13/cobra"

type ProposalsConfig struct {
	ProposalAllowList []uint `mapstructure:"proposal-allow-list"`
	ClydeProposal     uint   `mapstructure:"clyde-proposal"`
	GraceProposal     uint   `mapstructure:"grace-proposal"`
	MattProposal      uint   `mapstructure:"matt-proposal"`
}

func SetupProposalFlags(proposalsConfig *ProposalsConfig, cmd *cobra.Command) {
	cmd.PersistentFlags().UintSliceVar(&proposalsConfig.ProposalAllowList, "proposals.proposal-allow-list", []uint{}, "proposals to index")
	cmd.PersistentFlags().UintVar(&proposalsConfig.ClydeProposal, "proposals.clyde-proposal", 0, "proposal for clyde")
	cmd.PersistentFlags().UintVar(&proposalsConfig.GraceProposal, "proposals.grace-proposal", 0, "proposal for grace")
	cmd.PersistentFlags().UintVar(&proposalsConfig.MattProposal, "proposals.matt-proposal", 0, "proposal for matt")
}
