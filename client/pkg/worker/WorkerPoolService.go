package worker

import (
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/bean"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/connection"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/constants"
	"go.uber.org/zap"
)

type PoolService interface {
	Start(count int)
	Stop()
	SubmitJob(record bean.Record)
}

type PoolServiceImpl struct {
	logger            *zap.SugaredLogger
	workers           []*Worker
	jobChannel        chan bean.Record
	connectionService connection.ConnectionService
}

func NewPoolService(logger *zap.SugaredLogger,
	connectionService connection.ConnectionService) *PoolServiceImpl {
	return &PoolServiceImpl{
		logger:            logger,
		workers:           make([]*Worker, 0),
		jobChannel:        make(chan bean.Record, constants.MaxRequests),
		connectionService: connectionService,
	}
}

func (pool *PoolServiceImpl) Start(count int) {
	for i := 0; i < count; i++ {
		worker := NewWorker(i, pool.jobChannel, pool.connectionService)
		pool.workers = append(pool.workers, worker)
		worker.Start()
	}
}

func (pool *PoolServiceImpl) Stop() {
	for _, worker := range pool.workers {
		worker.Stop()
	}
}

func (pool *PoolServiceImpl) SubmitJob(record bean.Record) {
	pool.jobChannel <- record
}
