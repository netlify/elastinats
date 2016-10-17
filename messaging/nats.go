package messaging

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/nats-io/nats"
)

type NatsConfig struct {
	CAFiles  []string `mapstructure:"ca_files"  json:"ca_files"`
	KeyFile  string   `mapstructure:"key_file"  json:"key_file"`
	CertFile string   `mapstructure:"cert_file" json:"cert_file"`
	Servers  []string `mapstructure:"servers"   json:"servers"`
}

// TLSConfig will load the TLS certificate
func (cfg *NatsConfig) TLSConfig() (*tls.Config, error) {
	pool := x509.NewCertPool()
	for _, caFile := range cfg.CAFiles {
		caData, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}

		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("Failed to add CA cert at %s", caFile)
		}
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	return tlsConfig, nil
}

// ServerString will build the proper string for nats connect
func (config *NatsConfig) ServerString() string {
	return strings.Join(config.Servers, ",")
}

// ConnectToNats will do a TLS connection to the nats servers specified
func ConnectToNats(config *NatsConfig) (*nats.Conn, error) {
	tlsConfig, err := config.TLSConfig()
	if err != nil {
		return nil, err
	}

	return nats.Connect(config.ServerString(), nats.Secure(tlsConfig))
}
