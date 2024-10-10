package tcpClient

import (
	"context"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/config"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/connection"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/constants"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/processing"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/storage"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/worker"
	"go.uber.org/zap"
)

type TCPClientService interface {
	Start(ctx context.Context) error
	Stop()
}

type TCPClientServiceImpl struct {
	logger            *zap.SugaredLogger
	configService     config.ConfigService
	storageService    storage.StorageService
	connectionService connection.ConnectionService
	workerPoolService worker.PoolService
	processingService processing.ProcessingService
}

func NewTCPClientServiceImpl(logger *zap.SugaredLogger,
	configService config.ConfigService,
	storageService storage.StorageService,
	connectionService connection.ConnectionService,
	workerPoolService worker.PoolService,
	processingService processing.ProcessingService) *TCPClientServiceImpl {
	return &TCPClientServiceImpl{
		logger:            logger,
		configService:     configService,
		storageService:    storageService,
		connectionService: connectionService,
		workerPoolService: workerPoolService,
		processingService: processingService,
	}
}

func (impl *TCPClientServiceImpl) Start(ctx context.Context) error {
	impl.logger.Info("starting TCP client")

	configs, err := impl.configService.LoadConfig(constants.ConfigFilePath)
	if err != nil {
		impl.logger.Errorw("failed to load config", "error", err)
		return err
	}

	impl.storageService.SimulateRecords(constants.TotalRecords)

	err = impl.connectionService.EstablishConnections(constants.MaxConnections)
	if err != nil {
		impl.logger.Errorw("failed to establish connections", "error", err)
		return err
	}

	impl.workerPoolService.Start(constants.MaxConnections)

	impl.processingService.ProcessRecords(ctx, configs)

	return nil
}

func (impl *TCPClientServiceImpl) Stop() {
	impl.logger.Info("stopping TCP client")
	impl.workerPoolService.Stop()
	impl.connectionService.CloseAllConnections()
}
