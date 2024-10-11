package tcpServer

import (
	"bufio"
	"context"
	"fmt"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/connectionRepository"
	"github.com/ashishsonamm/rate-limited-data-processing-system/server/pkg/idGenerator"
	"go.uber.org/zap"
	"net"
	"strings"
	"sync"
)

type TCPServerService interface {
	StartServers(ctx context.Context, startingPort int, numServers int) error
	StopServers()
	HandleConnection(conn net.Conn)
}

type TCPServerServiceImpl struct {
	logger               *zap.SugaredLogger
	connectionRepository connectionRepository.ConnectionRepository
	idGeneratorService   idGenerator.IDGeneratorService
	listeners            []net.Listener
	acceptWg             sync.WaitGroup
	quitChannel          chan bool
}

func NewTCPServerService(
	logger *zap.SugaredLogger,
	connectionRepository connectionRepository.ConnectionRepository,
	idGeneratorService idGenerator.IDGeneratorService,
) *TCPServerServiceImpl {
	return &TCPServerServiceImpl{
		logger:               logger,
		connectionRepository: connectionRepository,
		idGeneratorService:   idGeneratorService,
		listeners:            make([]net.Listener, 0),
		quitChannel:          make(chan bool),
	}
}

func (impl *TCPServerServiceImpl) StartServers(ctx context.Context, startingPort int, numServers int) error {
	for i := 0; i < numServers; i++ {
		port := startingPort + i
		impl.logger.Infow("starting TCP server", "port", port)

		var err error
		addr := fmt.Sprintf(":%d", port)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			impl.logger.Errorw("failed to start TCP server", "port", port, "error", err)
			return err
		}

		impl.listeners = append(impl.listeners, listener)

		impl.acceptWg.Add(1)
		go impl.acceptConnections(ctx, listener)
	}

	return nil
}

func (impl *TCPServerServiceImpl) acceptConnections(ctx context.Context, listener net.Listener) {
	defer impl.acceptWg.Done()

	for {
		select {
		case <-impl.quitChannel:
			return
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				impl.logger.Errorw("error accepting connection", "error", err)
				continue
			}

			impl.connectionRepository.SaveConnection(conn)
			go impl.HandleConnection(conn)
		}
	}
}

func (impl *TCPServerServiceImpl) StopServers() {
	impl.logger.Info("stopping TCP server")
	close(impl.quitChannel)
	for _, listener := range impl.listeners {
		if listener != nil {
			listener.Close()
		}
	}
	impl.acceptWg.Wait()
}

func (impl *TCPServerServiceImpl) HandleConnection(conn net.Conn) {
	defer conn.Close()
	defer impl.connectionRepository.RemoveConnection(conn)

	reader := bufio.NewReader(conn)
	for {
		data, err := reader.ReadString('\n')
		if err != nil {
			impl.logger.Errorw("error reading from connection", "error", err)
			return
		}

		data = strings.TrimSpace(data)
		id := impl.idGeneratorService.GenerateIDAtomically()

		response := fmt.Sprintf("%d,%s\n", id, data)
		_, err = conn.Write([]byte(response))
		if err != nil {
			impl.logger.Errorw("error writing to connection", "error", err)
			return
		}

		impl.logger.Infow("processed request", "data", data, "responseID", id)
	}
}
