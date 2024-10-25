package main

import (
	"context"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/connectionRepository"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/constants"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/idGenerator"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/tcpServer"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	connectionRepo := connectionRepository.NewConnectionRepository()
	idGeneratorService := idGenerator.NewIDGeneratorService()

	server := tcpServer.NewTCPServer(sugaredLogger, connectionRepo, idGeneratorService)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := server.StartServers(ctx, constants.StartingPort, constants.TotalServers)
	if err != nil {
		sugaredLogger.Fatalf("Failed to start servers", "err", err)
	}

	// graceful shutdown
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChannel:
		cancel()
	}

	server.StopServers()
}
