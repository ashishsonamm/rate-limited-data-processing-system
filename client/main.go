package main

import (
	"context"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/config"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/connection"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/processing"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/storage"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/tcpClient"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/worker"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	configService := config.NewConfigServiceImpl(sugaredLogger)
	storageService := storage.NewStorageService(sugaredLogger)
	connectionService := connection.NewConnectionService(sugaredLogger)
	workerPoolService := worker.NewPoolService(sugaredLogger, connectionService)
	processingService := processing.NewProcessingService(sugaredLogger, storageService, workerPoolService)

	client := tcpClient.NewTCPClientServiceImpl(
		sugaredLogger,
		configService,
		storageService,
		connectionService,
		workerPoolService,
		processingService,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := client.Start(ctx)
	if err != nil {
		sugaredLogger.Fatalw("Failed to start client", "error", err)
	}

	// graceful shutdown
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChannel:
		cancel()
	}
	client.Stop()
}
