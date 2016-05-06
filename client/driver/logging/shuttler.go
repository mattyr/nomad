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
	sConfig.ComputeHeader()
	logger.Printf("[DEBUG] log-shuttle config: %+v\n", sConfig)
	s := shuttle.NewShuttle(sConfig)

	pr, pw := io.Pipe()
	s.Logger = logger
	s.ErrLogger = logger
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
	sConfig.InputFormat = shuttle.InputFormatRaw
	sConfig.UseGzip = config.UseGzip
	sConfig.Drop = config.Drop
	sConfig.Prival = shuttle.DefaultPriVal
	sConfig.Version = shuttle.DefaultVersion
	sConfig.Procid = config.Procid
	sConfig.Appname = config.Appname
	sConfig.Appname = config.LogplexToken
	sConfig.Hostname = config.Hostname
	sConfig.Msgid = shuttle.DefaultMsgID
	sConfig.LogsURL = config.LogsURL
	sConfig.StatsSource = config.StatsSource
	sConfig.StatsInterval = config.StatsInterval
	sConfig.WaitDuration = shuttle.DefaultWaitDuration
	sConfig.Timeout = shuttle.DefaultTimeout
	sConfig.MaxAttempts = config.MaxAttempts
	sConfig.NumOutlets = config.NumOutlets
	sConfig.BatchSize = config.BatchSize
	sConfig.BackBuff = config.BackBuff
	sConfig.MaxLineLength = config.MaxLineLength
	sConfig.KinesisShards = config.KinesisShards
	return sConfig
}
