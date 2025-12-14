package config

import (
	"strings"

	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Node     NodeConfig     `mapstructure:"node"`
	Ingest   IngestConfig   `mapstructure:"ingest"`
	Log      LogConfig      `mapstructure:"log"`
}

type LogConfig struct {
	Level    string `mapstructure:"level"`
	FilePath string `mapstructure:"file_path"`
}

type ServerConfig struct {
	ListenAddr         string   `mapstructure:"listen_addr"`
	CORSAllowedOrigins []string `mapstructure:"cors_allowed_origins"`
	ExcludedAccounts   []string `mapstructure:"excluded_accounts"`
}

type DatabaseConfig struct {
	ClickHouseAddr     string `mapstructure:"clickhouse_addr"`
	ClickHouseDB       string `mapstructure:"clickhouse_db"`
	ClickHouseUser     string `mapstructure:"clickhouse_user"`
	ClickHousePassword string `mapstructure:"clickhouse_password"`
	PostgresConn       string `mapstructure:"postgres_conn"`
}

type NodeConfig struct {
	RPC  string `mapstructure:"rpc"`
	GRPC string `mapstructure:"grpc"`
	LCD  string `mapstructure:"lcd"`
}

type IngestConfig struct {
	BlockPollInterval      time.Duration `mapstructure:"block_poll_interval"`
	BackfillInterval       time.Duration `mapstructure:"backfill_interval"`
	BackfillBatchSize      int64         `mapstructure:"backfill_batch_size"`
	BackfillWorkers        int           `mapstructure:"backfill_workers"`
	RichlistUpdateInterval time.Duration `mapstructure:"richlist_update_interval"`
	StartHeight            int64         `mapstructure:"start_height"`
	EndHeight              int64         `mapstructure:"end_height"`
	FillGaps               bool          `mapstructure:"fill_gaps"`
}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("server.listen_addr", ":3000")
	v.SetDefault("server.cors_allowed_origins", []string{"*"})
	v.SetDefault("database.clickhouse_addr", "localhost:9000")
	v.SetDefault("database.clickhouse_db", "default")
	v.SetDefault("database.clickhouse_user", "default")
	v.SetDefault("database.clickhouse_password", "")
	v.SetDefault("database.postgres_conn", "postgres://user:password@localhost:5432/fcd")
	v.SetDefault("node.rpc", "http://localhost:26657")
	v.SetDefault("node.grpc", "localhost:9090")
	v.SetDefault("node.lcd", "")
	v.SetDefault("ingest.block_poll_interval", "1s")
	v.SetDefault("ingest.backfill_interval", "0s")
	v.SetDefault("ingest.backfill_batch_size", 10)
	v.SetDefault("ingest.backfill_workers", 5) // 0 = auto-calculate based on batch size
	v.SetDefault("ingest.richlist_update_interval", "1h")
	v.SetDefault("ingest.start_height", 0)
	v.SetDefault("ingest.end_height", 0)
	v.SetDefault("ingest.fill_gaps", false)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.file_path", "")

	// Config file
	v.SetConfigName("config")
	v.SetConfigType("toml")
	v.AddConfigPath(".")
	v.AddConfigPath(path)

	// Environment variables
	v.SetEnvPrefix("FCD")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
		// Config file not found is okay if we use env vars or defaults
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
