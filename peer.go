package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	tcpPort = 8080
	udpPort = 8081
)

var (
	readChannel  = make(chan InboundMessage)
	writeChannel = make(chan OutboundMessage)
	tcpIngress   = NewTCPConnectionPool(tcpPort, Incoming)
	tcpEgress    = NewTCPConnectionPool(tcpPort, Outgoing)
	udpEgress    = NewUDPConnectionPool(udpPort, Outgoing)
	leader       = &SafeValue[string]{}
	members      = &SafeList[int]{}
	aliveMembers = &SafeList[int]{}
	pendingOps   = &SafeList[RequestMessage]{}
	viewId       = SafeValue[int]{}
	requestId    = SafeValue[int]{}
	peerIdToName = make(map[string]int)
	peerNameToId = make(map[int]string)
	beats        = &sync.Map{}
	ackCount     = &sync.Map{}
	sent         = &SafeValue[int]{}
)

type RequestMessage struct {
	ViewId    int
	RequestId int
	Operation OperationType
	PeerId    int
}

type SafeList[T any] struct {
	list []T
	lock sync.Mutex
}

func (sl *SafeList[T]) Contains(value T) bool {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for _, v := range sl.list {
		if reflect.DeepEqual(v, value) {
			return true
		}
	}
	return false
}

func (sl *SafeList[T]) Sort(less func(a, b T) bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sort.Slice(sl.list, func(i, j int) bool {
		return less(sl.list[i], sl.list[j])
	})
}

func (sl *SafeList[T]) Replace(newList []T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.list = newList
}

func (sl *SafeList[T]) GetByViewAndRequestId(viewId, requestId int) (RequestMessage, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for _, msg := range sl.list {
		if rm, ok := any(msg).(RequestMessage); ok {
			if rm.ViewId == viewId && rm.RequestId == requestId {
				return rm, true
			}
		}
	}
	var zero RequestMessage
	return zero, false
}

func (sl *SafeList[T]) RemoveByViewAndRequestId(viewId, requestId int) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for i, msg := range sl.list {
		if rm, ok := any(msg).(RequestMessage); ok {
			if rm.ViewId == viewId && rm.RequestId == requestId {
				sl.list = append(sl.list[:i], sl.list[i+1:]...)
				return
			}
		}
	}
}

func (sl *SafeList[T]) RemoveByValue(value T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for i, v := range sl.list {
		if reflect.DeepEqual(v, value) {
			sl.list = append(sl.list[:i], sl.list[i+1:]...)
			return
		}
	}
}

func (sl *SafeList[T]) Add(value T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.list = append(sl.list, value)
}

func (sl *SafeList[T]) Remove(index int) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if index >= 0 && index < len(sl.list) {
		sl.list = append(sl.list[:index], sl.list[index+1:]...)
	}
}

func (sl *SafeList[T]) Get(index int) (T, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if index >= 0 && index < len(sl.list) {
		return sl.list[index], true
	}
	var zero T
	return zero, false
}

func (sl *SafeList[T]) GetAll() []T {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	return append([]T(nil), sl.list...)
}

type SafeValue[T any] struct {
	Value T
	Lock  sync.Mutex
}

func (sv *SafeValue[T]) Get() T {
	sv.Lock.Lock()
	defer sv.Lock.Unlock()
	return sv.Value
}

func (sv *SafeValue[T]) Set(value T) {
	sv.Lock.Lock()
	defer sv.Lock.Unlock()
	sv.Value = value
}

type InboundMessage struct {
	Data   []byte
	Sender net.Addr
}

type Protocol int

const (
	TCP Protocol = 0
	UDP Protocol = 1
)

type MessageType int

const (
	REQ             MessageType = 0
	OK              MessageType = 1
	HEARTBEAT       MessageType = 2
	NEWLEADER       MessageType = 3
	JOIN            MessageType = 4
	NEWVIEW         MessageType = 5
	PENDINGRESPONSE MessageType = 6
)

type OperationType int

const (
	ADD     OperationType = 0
	DEL     OperationType = 1
	NOTHING OperationType = 2
	PENDING OperationType = 3
)

type OutboundMessage struct {
	Data      []byte
	Recipient net.Addr
	Protocol  Protocol
}

type OKMessage struct {
	RequestId int
	ViewId    int
}

type NewViewMessage struct {
	ViewId  int
	members []int
}

type NewLeaderMessage struct {
	RequesId  int
	ViewId    int
	Operation OperationType
}

type PendingResponseMessage struct {
	RequestId int
	ViewId    int
	Operation OperationType
	PeerId    int
}

type ConnectionType int

const (
	Incoming ConnectionType = 0
	Outgoing ConnectionType = 1
)

type ConnectionPool struct {
	Connections      sync.Map // Using sync.Map for concurrent access
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

func GetAddrFromHostname(hostname string) (net.Addr, error) {
	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses found for hostname: %s", hostname)
	}
	return &net.TCPAddr{IP: addrs[0]}, nil
}

// ReadPeers reads the list of peers from a file and excludes the host itself from the list.
func GetPeers(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var peers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		peer := strings.TrimSpace(scanner.Text())
		if peer != "" {
			peers = append(peers, peer)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return peers, nil
}

func GetPeerNameToIdMapping(peers []string) map[string]int {
	peerNameToId := make(map[string]int)
	for i, peer := range peers {
		peerNameToId[peer] = i + 1
	}
	return peerNameToId
}

func GetPeerIdToNameMapping(peers []string) map[int]string {
	peerIdToName := make(map[int]string)
	for i, peer := range peers {
		peerIdToName[i+1] = peer
	}
	return peerIdToName
}

func RemoveSelf(peers []string) ([]string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	var peersWithoutSelf []string
	for _, peer := range peers {
		if peer != hostname {
			peersWithoutSelf = append(peersWithoutSelf, peer)
		}
	}

	return peersWithoutSelf, nil
}

func GetLeader(peers []string) (string, error) {
	if len(peers) == 0 {
		return "", nil
	}
	return peers[0], nil
}

func GetTCPConnection(addr net.Addr, port int) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr.(*net.TCPAddr).IP.String(), port))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func ListenForTCPConnections(port int, tcpIngress *ConnectionPool, readChannel chan InboundMessage) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("Error starting TCP listener:", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		tcpIngress.Add(conn.RemoteAddr(), conn)
		go HandleTCPConnection(conn, readChannel)
	}
}

func HandleTCPConnection(conn net.Conn, readChannel chan InboundMessage) {
	defer conn.Close()
	// fmt.Println("Accepted new TCP connection from", conn.RemoteAddr())

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from TCP connection:", err)
			}
			break
		}
		message := InboundMessage{
			Data:   buffer[:n],
			Sender: conn.RemoteAddr(),
		}
		readChannel <- message
	}
}

func ListenForUDPMessages(port int, readChannel chan InboundMessage) {
	addr := net.UDPAddr{
		Port: port,
		IP:   net.IPv4zero,
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Println("Error starting UDP listener:", err)
		return
	}
	defer conn.Close()

	// fmt.Println("Listening for incoming UDP messages on port", port)

	buffer := make([]byte, 1024)
	for {
		n, sender, err := conn.ReadFrom(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP connection:", err)
			continue
		}
		message := InboundMessage{
			Data:   buffer[:n],
			Sender: sender,
		}
		readChannel <- message
	}
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

func SendMessageToHost(host string, data []byte, protocol Protocol, writeChannel chan OutboundMessage) {
	addr, err := GetAddrFromHostname(host)

	if err != nil {
		fmt.Printf("error resolving address for host %s: %v\n", host, err)
	}

	writeChannel <- OutboundMessage{
		Data:      data,
		Recipient: addr,
		Protocol:  protocol,
	}

	// fmt.Println("Added message to write channel for host", host)
}

func SendMessageToMembers(members *SafeList[int], data []byte, protocol Protocol, peerIdToName map[int]string, writeChannel chan OutboundMessage, me string) {
	for _, member := range members.GetAll() {
		memberName, exists := peerIdToName[member]
		if !exists {
			fmt.Println("Error: member ID not found in peerIdToName map:", member)
			continue
		}
		if memberName == me {
			continue
		}
		go SendMessageToHost(memberName, data, protocol, writeChannel)
	}
}

func PeriodicSendMessageToMembers(members *SafeList[int], data []byte, t int, protocol Protocol, peerIdToName map[int]string, writeChannel chan OutboundMessage, me string) {
	ticker := time.NewTicker(time.Duration(t) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		go SendMessageToMembers(members, data, protocol, peerIdToName, writeChannel, me)
	}
}

func PeriodicSendBroadcaseMessage(data []byte, t float64, port int) {
	ticker := time.NewTicker(time.Duration(int64(t * 1e9)))
	defer ticker.Stop()

	for range ticker.C {
		err := BroadcastMessage(data, port)
		if err != nil {
			fmt.Println("Error broadcasting message:", err)
		}
	}
}

func BroadcastMessage(data []byte, port int) error {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   net.IPv4bcast,
		Port: port,
	})
	if err != nil {
		return fmt.Errorf("error creating UDP connection: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("error sending broadcast message: %v", err)
	}

	return nil
}

func MonitorHeartBeats(beats *sync.Map, members *SafeList[int], t float64, me string, leader *SafeValue[string], peerIdToName map[int]string, peerNameToId map[string]int, writeChannel chan OutboundMessage) {

	ticker := time.NewTicker(time.Duration(int64(t * 1e9)))
	defer ticker.Stop()

	// Start a thread for failure detection every 2T seconds
	for range ticker.C {
		membersList := members.GetAll()
		// fmt.Println(membersList)
		// Check if all heartbeats received
		for _, member := range membersList {
			if peerIdToName[member] == me {
				continue
			}
			// If a peer failed to send the heartbeat
			if beat, ok := beats.Load(peerIdToName[member]); !ok || !beat.(bool) {

				if peerIdToName[member] == leader.Get() {
					fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, message:\"peer %d (leader) unreachable\"}\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()], member)
				} else {
					fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, message:\"peer %d unreachable\"}\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()], member)
				}
				memberName := peerIdToName[member]

				if memberName != leader.Get() && me == leader.Get() {
					requestId.Set(requestId.Get() + 1)
					requestMessage := RequestMessage{
						RequestId: requestId.Get(),
						ViewId:    viewId.Get(),
						Operation: DEL,
						PeerId:    member,
					}
					pendingOps.Add(requestMessage)
					encodedData := EncodeIntegersToByteArray(int(REQ), requestMessage.RequestId, requestMessage.ViewId, int(requestMessage.Operation), requestMessage.PeerId)
					aliveMembers.Replace(members.GetAll())
					aliveMembers.RemoveByValue(member)
					go SendMessageToMembers(aliveMembers, encodedData, TCP, peerIdToName, writeChannel, me)
					if len(aliveMembers.GetAll()) == 1 {
						viewId.Set(viewId.Get() + 1)
						members.RemoveByValue(member)
						fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, memb_list: [<%s>]}\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()], strings.Trim(strings.Join(strings.Fields(fmt.Sprint(members.GetAll())), ","), "[]"))
					}
				} else if memberName == leader.Get() {

					// members.RemoveByValue(peerNameToId[leader.Get()])
					members.Sort(func(a, b int) bool { return a < b })
					if newLeaderId, ok := members.Get(1); ok {
						leader.Set(peerIdToName[newLeaderId])
					}
					if leader.Get() == me {
						aliveMembers.Replace(members.GetAll())
						aliveMembers.RemoveByValue(member)
						requestId.Set(requestId.Get() + 1)
						encodedData := EncodeIntegersToByteArray(int(NEWLEADER), requestId.Get(), viewId.Get(), int(PENDING))
						go SendMessageToMembers(aliveMembers, encodedData, TCP, peerIdToName, writeChannel, me)
						requestId.Set(requestId.Get() + 1)
						requestMessage := RequestMessage{
							ViewId:    viewId.Get(),
							RequestId: requestId.Get(),
							Operation: DEL,
							PeerId:    peerNameToId[memberName],
						}
						pendingOps.Add(requestMessage)
					}
				}
			}
		}
		// Reset the status for all members
		for _, member := range membersList {
			beats.Store(peerIdToName[member], false)
		}
	}

}

func EncodeIntegersToByteArray(integers ...int) []byte {
	var buffer bytes.Buffer
	for _, integer := range integers {
		err := binary.Write(&buffer, binary.LittleEndian, int32(integer))
		if err != nil {
			fmt.Println("Error encoding integer:", err)
			return nil
		}
	}
	return buffer.Bytes()
}

func DecodeByteArrayToIntegers(data []byte) ([]int, error) {
	var integers []int
	buffer := bytes.NewReader(data)
	for {
		var integer int32
		err := binary.Read(buffer, binary.LittleEndian, &integer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding integer: %v", err)
		}
		integers = append(integers, int(integer))
	}
	return integers, nil
}

func GetHostnameFromAddr(addr net.Addr) (string, error) {
	ip, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", err
	}

	names, err := net.LookupAddr(ip)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no hostnames found for IP: %s", ip)
	}
	return names[0], nil
}

func CleanHostname(hostname string) string {
	if idx := strings.Index(hostname, "."); idx != -1 {
		return hostname[:idx]
	}
	return hostname
}

func main() {

	// Parse command line arguments using flags
	hostsfile := flag.String("h", "", "Path to the hosts file")
	initialDelay := flag.Int("d", 0, "Initial delay in seconds (default: 0)")
	crashDelay := flag.Int("c", 0, "Crash delay in seconds")
	crash := flag.Bool("t", false, "Crash leader")

	flag.Parse()

	if *hostsfile == "" || *initialDelay < 0 {
		fmt.Println("Usage: go run peer.go -h <hostsfile> -d <initial_delay>")
		os.Exit(1)
	}

	me, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting hostname:", err)
		os.Exit(1)
	}

	peers, err := GetPeers(*hostsfile)
	if err != nil {
		fmt.Println("Error reading peers:", err)
		os.Exit(1)
	}

	peerNameToId := GetPeerNameToIdMapping(peers)
	peerIdToName := GetPeerIdToNameMapping(peers)

	leaderName, err := GetLeader(peers)

	if err != nil {
		fmt.Println("Error determining leader:", err)
		os.Exit(1)
	}

	leader.Set(leaderName)
	leaderId := peerNameToId[leaderName]
	if me == leaderName {
		members.Add(leaderId)
	}

	// peers, err = RemoveSelf(peers)
	// if err != nil {
	// 	fmt.Println("Error removing self from peers:", err)
	// 	os.Exit(1)
	// }

	time.Sleep(time.Duration(*initialDelay) * time.Second)

	// Setup a TCP connection with leader
	if me != leader.Get() {
		leaderAddr, err := GetAddrFromHostname(leader.Get())
		if err != nil {
			fmt.Println("Error resolving leader address:", err)
			os.Exit(1)
		}

		_, err = tcpEgress.Get(leaderAddr)
		if err != nil {
			fmt.Println("Error connecting to leader:", err)
			os.Exit(1)
		}
	}

	// Start listening for incoming TCP Connections
	go ListenForTCPConnections(tcpPort, tcpIngress, readChannel)

	// Peer: Send JOIN message to leader
	if me == leader.Get() {
		viewId.Set(viewId.Get() + 1)
		fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, memb_list: [<%s>]}\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()], strings.Trim(strings.Join(strings.Fields(fmt.Sprint(members.GetAll())), ","), "[]"))
	} else {
		encodedData := EncodeIntegersToByteArray(int(JOIN))
		go SendMessageToHost(leader.Get(), encodedData, TCP, writeChannel)
	}

	if (*crashDelay) > 0 {
		go func() {
			time.Sleep(time.Duration(*crashDelay) * time.Second)
			fmt.Printf("peer_id:%d, view_id: %d, leader: %d, message:\"crashing\"\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()])
			os.Exit(1)
		}()
	}

	go func() {
		// Wait for all peers to join the network
		for len(members.GetAll()) != len(peers) {
			time.Sleep(1 * time.Second)
		}

		if *crash {
			requestId.Set(requestId.Get() + 1)
			requestMessage := RequestMessage{
				ViewId:    viewId.Get(),
				RequestId: requestId.Get(),
				Operation: DEL,
				PeerId:    peerNameToId[peers[4]],
			}
			target := &SafeList[int]{list: members.GetAll()}
			target.RemoveByValue(peerNameToId[peers[1]])
			target.RemoveByValue(peerNameToId[peers[4]])
			sent.Set(0)
			go func() {
				SendMessageToMembers(target, EncodeIntegersToByteArray(int(REQ), requestMessage.RequestId, requestMessage.ViewId, int(requestMessage.Operation), requestMessage.PeerId), TCP, peerIdToName, writeChannel, me)
				for sent.Get() < len(peers)-3 {
					continue
				}
				os.Exit(1)
			}()
		}

		// Start listening for incoming UDP messages
		go ListenForUDPMessages(udpPort, readChannel)

		encodedData := EncodeIntegersToByteArray(int(HEARTBEAT))
		// Start a thread to send heartbeats every T seconds
		// go PeriodicSendMessageToMembers(members, encodedData, 5, UDP, peerIdToName, writeChannel, me)
		go PeriodicSendBroadcaseMessage(encodedData, 1.1, udpPort)
		// Identify the current list of members
		// Unicast the heartbeats using a single UDP connection
		go MonitorHeartBeats(beats, members, 2.2, me, leader, peerIdToName, peerNameToId, writeChannel)
	}()

	for {
		select {
		case inboundMessage := <-readChannel:
			data, err := DecodeByteArrayToIntegers(inboundMessage.Data)
			if err != nil {
				fmt.Println("Error decoding byte array to integers:", err)
				break
			}
			sender, err := GetHostnameFromAddr(inboundMessage.Sender)
			if err != nil {
				fmt.Println("Error getting hostname from address:", err)
				break
			}
			sender = CleanHostname(sender)
			switch MessageType(data[0]) {
			case REQ:
				requestMessage := RequestMessage{
					RequestId: data[1],
					ViewId:    data[2],
					Operation: OperationType(data[3]),
					PeerId:    data[4],
				}
				pendingOps.Add(requestMessage)
				okMessage := OKMessage{
					RequestId: data[1],
					ViewId:    data[2],
				}
				encodedData := EncodeIntegersToByteArray(int(OK), okMessage.RequestId, okMessage.ViewId)
				go SendMessageToHost(leader.Get(), encodedData, TCP, writeChannel)
			case OK:
				if me == leader.Get() {
					// fmt.Println("Received OK from", sender)
					key := fmt.Sprintf("%d-%d", data[2], data[1])
					if count, ok := ackCount.Load(key); ok {
						ackCount.Store(key, count.(int)+1)
					} else {
						ackCount.Store(key, 1)
					}

					op, ok := pendingOps.GetByViewAndRequestId(data[2], data[1])
					if !ok {
						fmt.Println("Error:Hello Operation not found in pendingOps")
						break
					}

					rcount := len(members.GetAll()) - 1

					if op.Operation == DEL {
						rcount--
					}

					// fmt.Println(key, sender)

					if count, ok := ackCount.Load(key); ok && count.(int) == rcount {
						// Send NewView message
						viewId.Set(viewId.Get() + 1)
						if op.Operation == ADD {
							members.Add(op.PeerId)
						} else if op.Operation == DEL {
							members.RemoveByValue(op.PeerId)
						}
						encodedData := EncodeIntegersToByteArray(append([]int{int(NEWVIEW), viewId.Get()}, members.GetAll()...)...)
						go SendMessageToMembers(members, encodedData, TCP, peerIdToName, writeChannel, me)
						pendingOps.RemoveByViewAndRequestId(data[2], data[1])
					}
				}
			case HEARTBEAT:
				if members.Contains(peerNameToId[sender]) {
					// fmt.Println("Received heartbeat from", sender)
					beats.Store(sender, true)
				}
			case NEWLEADER:
				// Send pending operations to new leader
				leader.Set(sender)
				var pendingResponse []int
				pendingResponse = append(pendingResponse, int(PENDINGRESPONSE))
				if len(pendingOps.GetAll()) == 0 {
					pendingResponse = append(pendingResponse, data[1], data[2], int(NOTHING), -1)
				} else {
					op, _ := pendingOps.Get(0)
					pendingResponse = append(pendingResponse, data[1], data[2], int(op.Operation), op.PeerId)
					pendingOps.Replace([]RequestMessage{})
				}
				encodedData := EncodeIntegersToByteArray(pendingResponse...)
				go SendMessageToHost(sender, encodedData, TCP, writeChannel)
			case JOIN:
				if len(members.GetAll()) == 1 {
					members.Add(peerNameToId[sender])
					viewId.Set(viewId.Get() + 1)
					encodedData := EncodeIntegersToByteArray(append([]int{int(NEWVIEW), viewId.Get()}, members.GetAll()...)...)
					go SendMessageToMembers(members, encodedData, TCP, peerIdToName, writeChannel, me)
				} else {
					requestId.Set(requestId.Get() + 1)
					requestMessage := RequestMessage{
						RequestId: requestId.Get(),
						ViewId:    viewId.Get(),
						Operation: ADD,
						PeerId:    peerNameToId[sender],
					}
					encodedData := EncodeIntegersToByteArray(int(REQ), requestMessage.RequestId, requestMessage.ViewId, int(requestMessage.Operation), requestMessage.PeerId)
					go SendMessageToMembers(members, encodedData, TCP, peerIdToName, writeChannel, me)
					pendingOps.Add(requestMessage)
				}
			case NEWVIEW:
				viewId.Set(data[1])
				membersList := data[2:]
				members.Replace(membersList)
				pendingOps.Remove(0)
				fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, memb_list: [<%s>]}\n", peerNameToId[me], viewId.Get(), peerNameToId[leader.Get()], strings.Trim(strings.Join(strings.Fields(fmt.Sprint(membersList)), ","), "[]"))
			case PENDINGRESPONSE:
				requestMessage := RequestMessage{
					ViewId:    data[2],
					RequestId: data[1],
					Operation: OperationType(data[3]),
					PeerId:    data[4],
				}
				_, ok := pendingOps.GetByViewAndRequestId(requestMessage.ViewId, requestMessage.RequestId)

				if !ok && requestMessage.Operation != NOTHING {
					pendingOps.Add(requestMessage)
				}

				key := fmt.Sprintf("%d-%d", data[2], data[1])
				if count, ok := ackCount.Load(key); ok {
					ackCount.Store(key, count.(int)+1)
				} else {
					ackCount.Store(key, 1)
				}

				if requestMessage.Operation == NOTHING {
					break
				}

				op, ok := pendingOps.GetByViewAndRequestId(data[2], data[1])
				if !ok {
					fmt.Println("Error:World Operation not found in pendingOps")
					break
				}

				rcount := len(aliveMembers.GetAll()) - 1

				if count, ok := ackCount.Load(key); ok && count.(int) == rcount {
					// Send NewView message
					viewId.Set(viewId.Get() + 1)
					if op.Operation == ADD {
						members.Add(op.PeerId)
					} else if op.Operation == DEL {
						members.RemoveByValue(op.PeerId)
						aliveMembers.RemoveByValue(op.PeerId)
					}
					encodedData := EncodeIntegersToByteArray(append([]int{int(NEWVIEW), viewId.Get()}, members.GetAll()...)...)
					go SendMessageToMembers(aliveMembers, encodedData, TCP, peerIdToName, writeChannel, me)
					pendingOps.RemoveByViewAndRequestId(data[2], data[1])

					for _, op := range pendingOps.GetAll() {
						encodedData := EncodeIntegersToByteArray(int(REQ), op.RequestId, op.ViewId, int(op.Operation), op.PeerId)
						go func() {
							if op.Operation == DEL {
								aliveMembers.RemoveByValue(op.PeerId)
							}
							SendMessageToMembers(aliveMembers, encodedData, TCP, peerIdToName, writeChannel, me)
						}()
					}

				}

			}

		case outboundMessage := <-writeChannel:
			switch outboundMessage.Protocol {
			case TCP:
				conn, err := tcpEgress.Get(outboundMessage.Recipient)
				if err != nil {
					// fmt.Println("Error getting TCP connection:", err)
					continue
				}
				tcpConn := conn.(net.Conn)
				_, err = tcpConn.Write([]byte(outboundMessage.Data))
				if err != nil {
					// fmt.Println("Error sending TCP message:", err)
				}
				if *crash {
					sent.Set(sent.Get() + 1)
				}
			case UDP:
				conn, err := udpEgress.Get(outboundMessage.Recipient)
				if err != nil {
					fmt.Println("Error getting UDP connection:", err)
					continue
				}
				udpConn := conn.(*net.UDPConn)
				_, err = udpConn.Write([]byte(outboundMessage.Data))
				if err != nil {
					fmt.Println("Error sending UDP message:", err)
				}
			}
		}
	}
}
