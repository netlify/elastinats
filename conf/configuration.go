package conf

import (
	"os"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"bytes"
	"errors"
	"time"

	"github.com/netlify/messaging"
)

type Config struct {
	NatsConf    messaging.NatsConfig `json:"nats_conf"`
	ElasticConf *ElasticConfig       `json:"elastic_conf"`
	LogConf     LoggingConfig        `json:"log_conf"`
	Subjects    []SubjectAndGroup    `json:"subjects"`
	ReportSec   int64                `json:"report_sec"`
}

type SubjectAndGroup struct {
	Subject  string         `json:"subject"`
	Group    string         `json:"group"`
	Endpoint *ElasticConfig `json:"endpoint"`
}

type ElasticConfig struct {
	Index           string   `json:"index"`
	Hosts           []string `json:"hosts"`
	Port            int      `json:"port"`
	Type            string   `json:"type"`
	BatchSize       int      `json:"batch_size"`
	BatchTimeoutSec int      `json:"batch_timeout_sec"`

	indexTemplate *template.Template
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

	config, err = populateConfig(config)
	if err != nil {
		return nil, err
	}

	return validateConfig(config)
}

func validateConfig(config *Config) (*Config, error) {
	// TODO
	return config, nil
}
