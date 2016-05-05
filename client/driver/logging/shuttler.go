package logging

import (
	"io"
	"log"

	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/heroku/log-shuttle"
)

type Shuttler struct {
	logShuttler *shuttle.Shuttle
	pr          io.Reader
	pw          io.Writer
}

func NewShuttler(config *structs.LogShuttleConfig, logger *log.Logger) (*Shuttler, error) {
	sConfig := getShuttleConfig(config)
	s := shuttle.NewShuttle(sConfig)

	pr, pw := io.Pipe()
	s.LoadReader(pr)

	s.Launch()

	// TODO:
	//go LogFmtMetricsEmitter(s.MetricsRegistry, sConfig.StatsSource, sConfig.StatsInterval, logger)

	return &Shuttler{logShuttler: s, pr: pr, pw: pw}, nil
}

func (s *Shuttler) Write(p []byte) (n int, err error) {
	return s.pw.Write(p)
}

func (s *Shuttler) Shutdown() {
	s.logShuttler.Land()
}

func getShuttleConfig(config *structs.LogShuttleConfig) shuttle.Config {
	sConfig := shuttle.NewConfig()
	sConfig.UseGzip = config.UseGzip
	sConfig.Drop = config.Drop
	sConfig.Prival = config.Prival
	sConfig.Version = config.Version
	sConfig.Procid = config.Procid
	sConfig.Appname = config.Appname
	sConfig.Appname = config.LogplexToken
	sConfig.Hostname = config.Hostname
	sConfig.Msgid = config.Msgid
	sConfig.LogsURL = config.LogsURL
	sConfig.StatsSource = config.StatsSource
	sConfig.StatsInterval = config.StatsInterval
	sConfig.WaitDuration = config.WaitDuration
	sConfig.Timeout = config.Timeout
	sConfig.MaxAttempts = config.MaxAttempts
	sConfig.NumOutlets = config.NumOutlets
	sConfig.BatchSize = config.BatchSize
	sConfig.BackBuff = config.BackBuff
	sConfig.MaxLineLength = config.MaxLineLength
	sConfig.KinesisShards = config.KinesisShards
	return sConfig
}
