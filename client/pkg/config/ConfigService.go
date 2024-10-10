package config

import (
	"encoding/csv"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/bean"
	"go.uber.org/zap"
	"os"
	"strconv"
)

type ConfigService interface {
	LoadConfig(filename string) ([]bean.Config, error)
}

type ConfigServiceImpl struct {
	logger *zap.SugaredLogger
}

func NewConfigServiceImpl(logger *zap.SugaredLogger) *ConfigServiceImpl {
	return &ConfigServiceImpl{
		logger: logger,
	}
}

func (impl *ConfigServiceImpl) LoadConfig(filename string) ([]bean.Config, error) {
	impl.logger.Infow("loading config", "filename", filename)

	file, err := os.Open(filename)
	if err != nil {
		impl.logger.Errorw("error opening file", "filename", filename)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		impl.logger.Errorw("error reading records", "filename", filename)
		return nil, err
	}

	var configs []bean.Config
	for _, record := range records {
		tps, err := strconv.Atoi(record[1])
		if err != nil {
			impl.logger.Errorw("error converting tps string to int", "tps", record[1])
			return nil, err
		}
		configs = append(configs, bean.Config{Scenario: record[0], TPS: tps})
	}

	return configs, nil
}
