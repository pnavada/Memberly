package network

import (
	"fmt"
	"net"
	"sync"
)

type ConnectionType int

const (
	Incoming ConnectionType = iota
	Outgoing
)

type ConnectionPool struct {
	Connections      sync.Map
	Port             int
	ConnectionType   ConnectionType
	GetNewConnection func(net.Addr, int) (interface{}, error)
}

func (cp *ConnectionPool) Add(addr net.Addr, conn interface{}) {
	cp.Connections.Store(addr, conn)
}

func (cp *ConnectionPool) Remove(addr net.Addr) {
	cp.Connections.Delete(addr)
}

func (cp *ConnectionPool) Get(addr net.Addr) (interface{}, error) {
	conn, exists := cp.Connections.Load(addr)
	if !exists && cp.ConnectionType == Outgoing {
		var err error
		conn, err = cp.GetNewConnection(addr, cp.Port)
		if err != nil {
			return nil, err
		}
		cp.Add(addr, conn)
	}
	return conn, nil
}

func NewTCPConnectionPool(Port int, ConnectionType ConnectionType) *ConnectionPool {
	return &ConnectionPool{
		Port:           Port,
		ConnectionType: ConnectionType,
		GetNewConnection: func(addr net.Addr, Port int) (interface{}, error) {
			return GetTCPConnection(addr, Port)
		},
	}
}

func NewUDPConnectionPool(Port int, ConnectionType ConnectionType) *ConnectionPool {
	return &ConnectionPool{
		Port:           Port,
		ConnectionType: ConnectionType,
		GetNewConnection: func(addr net.Addr, Port int) (interface{}, error) {
			return GetUDPConnection(addr, Port)
		},
	}
}

func GetTCPConnection(addr net.Addr, port int) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr.(*net.TCPAddr).IP.String(), port))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func GetUDPConnection(addr net.Addr, port int) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr.(*net.TCPAddr).IP.String(), port))
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
