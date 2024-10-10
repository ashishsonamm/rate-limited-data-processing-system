package connectionRepository

import (
	"net"
	"sync"
)

type ConnectionRepository interface {
	SaveConnection(conn net.Conn)
	RemoveConnection(conn net.Conn)
}

type ConnectionRepositoryImpl struct {
	connections sync.Map
}

func NewConnectionRepository() *ConnectionRepositoryImpl {
	return &ConnectionRepositoryImpl{
		connections: sync.Map{},
	}
}

func (impl *ConnectionRepositoryImpl) SaveConnection(conn net.Conn) {
	impl.connections.Store(conn.RemoteAddr(), conn)
}

func (impl *ConnectionRepositoryImpl) RemoveConnection(conn net.Conn) {
	impl.connections.Delete(conn.RemoteAddr())
}
