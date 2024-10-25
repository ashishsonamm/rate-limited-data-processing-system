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
	GetConnection() (net.Conn, error)
	//ReleaseConnection(conn net.Conn)
	CloseAllConnections()
}

type ConnectionServiceImpl struct {
	logger *zap.SugaredLogger
	//connections []net.Conn
	pool chan net.Conn
	mu   sync.Mutex
}

func NewConnectionService(logger *zap.SugaredLogger) *ConnectionServiceImpl {
	return &ConnectionServiceImpl{
		logger: logger,
		//connections: make([]net.Conn, 0),
		pool: make(chan net.Conn, constants.MaxConnections),
		mu:   sync.Mutex{},
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
		select {
		case impl.pool <- conn:
		default:
			conn.Close()
		}
		//impl.connections = append(impl.connections, conn)
	}

	if len(impl.pool) == 0 {
		impl.logger.Errorw("No connnections established")
		return errors.New("no connnections established")
	}

	return nil
}

func (impl *ConnectionServiceImpl) GetConnection() (net.Conn, error) {
	select {
	case conn := <-impl.pool:
		if conn == nil {
			return nil, errors.New("connection is nil")
		}
		impl.pool <- conn // add this connection back to the pool channel so that it becomes available to other workers
		return conn, nil
	}
}

//func (impl *ConnectionServiceImpl) ReleaseConnection(conn net.Conn) {
//	if conn == nil {
//		return
//	}
//
//	impl.mu.Lock()
//	defer impl.mu.Unlock()
//
//	select {
//	case impl.pool <- conn:
//		// Connection returned to pool
//	default:
//		// Pool is full, close the connection
//		conn.Close()
//	}
//}

func (impl *ConnectionServiceImpl) CloseAllConnections() {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	close(impl.pool)
	for conn := range impl.pool {
		conn.Close()
	}
	impl.pool = make(chan net.Conn, constants.MaxConnections)
}
