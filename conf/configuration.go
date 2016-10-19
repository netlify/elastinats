package conf

import (
	"bytes"
	"errors"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"os"

	"github.com/netlify/elastinats/messaging"
)

type Config struct {
	NatsConf    messaging.NatsConfig `mapstructure:"nats_conf"    json:"nats_conf"`
	ElasticConf *ElasticConfig       `mapstructure:"elastic_conf" json:"elastic_conf"`
	LogConf     LoggingConfig        `mapstructure:"log_conf"     json:"log_conf"`
	Subjects    []SubjectAndGroup    `mapstructure:"subjects"     json:"subjects"`
	ReportSec   int64                `mapstructure:"report_sec"   json:"report_sec"`
	BufferSize  int64                `mapstructure:"buffer_size"  json:"buffer_size"`
}

type SubjectAndGroup struct {
	Subject  string         `mapstructure:"subject"      json:"subject"`
	Group    string         `mapstructure:"group"        json:"group"`
	Endpoint *ElasticConfig `mapstructure:"elastic_conf" json:"endpoint"`
}

type ElasticConfig struct {
	Index           string   `mapstructure:"index"             json:"index"`
	Hosts           []string `mapstructure:"hosts"             json:"hosts"`
	Port            int      `mapstructure:"port"              json:"port"`
	Type            string   `mapstructure:"type"              json:"type"`
	BatchSize       int      `mapstructure:"batch_size"        json:"batch_size"`
	BatchTimeoutSec int      `mapstructure:"batch_timeout_sec" json:"batch_timeout_sec"`
	BufferSize      int      `mapstructure:"buffer_size"       json:"buffer_size"`
	indexTemplate   *template.Template
}

func (e *ElasticConfig) GetIndex(t time.Time) (string, error) {
	if e.Index == "" {
		return "", errors.New("No index configured")
	}

	if e.indexTemplate == nil {
		var err error
		e.indexTemplate, err = template.New("index_template").Parse(e.Index)
		if err != nil {
			return "", err
		}
	}

	b := bytes.NewBufferString("")
	err := e.indexTemplate.Execute(b, t)

	return strings.ToLower(b.String()), err
}

// LoadConfig loads the config from a file if specified, otherwise from the environment
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	viper.SetConfigType("json")

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return nil, err
	}

	viper.SetEnvPrefix("ELASTINATS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("./")
		viper.AddConfigPath("$HOME/.netlify-subscriptions/")
	}

	if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	config := new(Config)

	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	return config, nil
}
