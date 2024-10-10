package connection

import (
	"errors"
	"fmt"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/constants"
	"go.uber.org/zap"
	"net"
	"sync"
)

type ConnectionService interface {
	EstablishConnections(count int) error
	GetConnection() net.Conn
	CloseAllConnections()
}

type ConnectionServiceImpl struct {
	logger      *zap.SugaredLogger
	connections []net.Conn
	mu          sync.Mutex
}

func NewConnectionService(logger *zap.SugaredLogger) *ConnectionServiceImpl {
	return &ConnectionServiceImpl{
		logger:      logger,
		connections: make([]net.Conn, 0),
		mu:          sync.Mutex{},
	}
}

func (impl *ConnectionServiceImpl) EstablishConnections(count int) error {
	impl.logger.Infow("Establishing connections", "count", count)
	impl.mu.Lock()
	defer impl.mu.Unlock()

	for i := 0; i < count; i++ {
		port := constants.StartingPortForConnections + i
		addr := fmt.Sprintf("localhost:%d", port)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			impl.logger.Errorw("Failed to establish connection", "address", addr, "error", err)
			continue
		}
		impl.connections = append(impl.connections, conn)
	}

	if len(impl.connections) == 0 {
		impl.logger.Errorw("No connnections established")
		return errors.New("no connnections established")
	}

	return nil
}

func (impl *ConnectionServiceImpl) GetConnection() net.Conn {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	if len(impl.connections) == 0 {
		return nil
	}

	conn := impl.connections[0]
	impl.connections = append(impl.connections[1:], conn)
	return conn
}

func (impl *ConnectionServiceImpl) CloseAllConnections() {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	for _, conn := range impl.connections {
		conn.Close()
	}
	impl.connections = nil
}
