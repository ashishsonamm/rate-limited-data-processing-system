package processing

import (
	"context"
	"fmt"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/bean"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/constants"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/storage"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/worker"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

type ProcessingService interface {
	ProcessRecords(ctx context.Context, configs []bean.Config)
}

type ProcessingServiceImpl struct {
	logger         *zap.SugaredLogger
	storageService storage.StorageService
	workerService  worker.PoolService
}

func NewProcessingService(logger *zap.SugaredLogger, storageService storage.StorageService, workerService worker.PoolService) *ProcessingServiceImpl {
	return &ProcessingServiceImpl{
		logger:         logger,
		storageService: storageService,
		workerService:  workerService,
	}
}

func (impl *ProcessingServiceImpl) ProcessRecords(ctx context.Context, configs []bean.Config) {
	for _, config := range configs {
		interval := time.Second / time.Duration(config.TPS)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for i := 0; i < config.TPS; i++ {
			select {
			case <-ticker.C:
				id := fmt.Sprintf("record_%d", rand.Intn(constants.TotalRecords))
				data, ok := impl.storageService.Get(id)
				if !ok {
					impl.logger.Errorw("record does not exist", "id", id)
					continue
				}
				record := bean.Record{
					ID:   id,
					Data: data,
				}
				impl.workerService.SubmitJob(record)
			case <-ctx.Done(): // Handle cancellation
				impl.logger.Infow("processing stopped due to context cancellation", "scenario", config.Scenario)
				return
			}
		}
		impl.logger.Infow("transactions completed for config", "second", config.Scenario)
	}
}
