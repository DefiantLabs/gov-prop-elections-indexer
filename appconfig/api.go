package appconfig

import (
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/spf13/cobra"
)

type LCDConfig struct {
	URL string `mapstructure:"url"`
}

type HostConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	LogLevel string `mapstructure:"log_level"`
}

type APIConfig struct {
	Database        config.Database
	Probe           config.Probe
	LCD             LCDConfig
	Host            HostConfig
	ProposalsConfig ProposalsConfig
}

func SetupLCDFlags(lcdConfig *LCDConfig, cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&lcdConfig.URL, "lcd.url", "", "node lcd endpoint")
}

func SetupHostFlags(hostConfig *HostConfig, cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&hostConfig.Host, "host.host", "localhost", "api hostname")
	cmd.PersistentFlags().IntVar(&hostConfig.Port, "host.port", 8000, "api port")
	cmd.PersistentFlags().StringVar(&hostConfig.LogLevel, "host.log-level", "info", "api log level")
}
