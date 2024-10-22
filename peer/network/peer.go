package network

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"peer/datastructures"
	"peer/types"
	"peer/utils"
)

const (
	tcpPort           = 8080
	udpPort           = 8081
	heartbeatInterval = 1.1 // seconds
	monitorInterval   = 2.2 // seconds
)

// Peer represents a node in the distributed system
type Peer struct {
	// Identity
	Me           string
	Leader       *datastructures.SafeValue[string]
	PeerIdToName map[int]string
	PeerNameToId map[string]int
	Peers        []string

	// Membership management
	Members      *datastructures.SafeList[int]
	AliveMembers *datastructures.SafeList[int]
	PendingOps   *datastructures.SafeList[types.RequestMessage]

	// State management
	ViewId    *datastructures.SafeValue[int]
	RequestId *datastructures.SafeValue[int]
	Beats     *sync.Map
	AckCount  *sync.Map
	Sent      *datastructures.SafeValue[int]
	OpsStatus *sync.Map

	// Communication channels
	ReadChannel  chan types.InboundMessage
	WriteChannel chan types.OutboundMessage

	// Connection pools
	TCPIngress *ConnectionPool
	TCPEgress  *ConnectionPool
	UDPEgress  *ConnectionPool
}

// NewPeer creates and initializes a new peer
func NewPeer(me, leaderName string, peerNameToId map[string]int, peerIdToName map[int]string, peers []string) *Peer {
	p := &Peer{
		// Identity initialization
		Me:           me,
		Leader:       &datastructures.SafeValue[string]{Value: leaderName},
		PeerIdToName: peerIdToName,
		PeerNameToId: peerNameToId,
		Peers:        peers,

		// State initialization
		Members:      &datastructures.SafeList[int]{},
		AliveMembers: &datastructures.SafeList[int]{},
		PendingOps:   &datastructures.SafeList[types.RequestMessage]{},
		ViewId:       &datastructures.SafeValue[int]{},
		RequestId:    &datastructures.SafeValue[int]{},
		Beats:        &sync.Map{},
		AckCount:     &sync.Map{},
		Sent:         &datastructures.SafeValue[int]{},
		OpsStatus:    &sync.Map{},

		// Channel initialization
		ReadChannel:  make(chan types.InboundMessage),
		WriteChannel: make(chan types.OutboundMessage),

		// Connection pool initialization
		TCPIngress: NewTCPConnectionPool(tcpPort, Incoming),
		TCPEgress:  NewTCPConnectionPool(tcpPort, Outgoing),
		UDPEgress:  NewUDPConnectionPool(udpPort, Outgoing),
	}

	if me == leaderName {
		p.Members.Add(peerNameToId[leaderName])
		p.AliveMembers.Add(peerNameToId[leaderName])
	}

	return p
}

// Core functionality
func (p *Peer) Start() {
	go p.ListenForTCPConnections()

	if p.isLeader() {
		p.initializeAsLeader()
	} else {
		p.joinNetwork()
	}

	go p.StartHeartbeatAndMonitoring()
}

func (p *Peer) isLeader() bool {
	return p.Me == p.Leader.Get()
}

func (p *Peer) initializeAsLeader() {
	p.ViewId.Set(p.ViewId.Get() + 1)
	p.logMembershipChange()
}

func (p *Peer) joinNetwork() {
	encodedData := types.EncodeIntegersToByteArray(int(types.JOIN))
	go p.SendMessageToHost(p.Leader.Get(), encodedData, types.TCP)
}

// Network communication
func (p *Peer) ListenForTCPConnections() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", tcpPort))
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
		p.TCPIngress.Add(conn.RemoteAddr(), conn)
		go p.HandleTCPConnection(conn)
	}
}

func (p *Peer) HandleTCPConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from TCP connection:", err)
			}
			break
		}
		p.ReadChannel <- types.InboundMessage{
			Data:   buffer[:n],
			Sender: conn.RemoteAddr(),
		}
	}
}

func (p *Peer) ListenForUDPMessages() {
	addr := net.UDPAddr{
		Port: udpPort,
		IP:   net.IPv4zero,
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Println("Error starting UDP listener:", err)
		return
	}
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {
		n, sender, err := conn.ReadFrom(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP connection:", err)
			continue
		}
		p.ReadChannel <- types.InboundMessage{
			Data:   buffer[:n],
			Sender: sender,
		}
	}
}

func (p *Peer) SendMessageToHost(host string, data []byte, protocol types.Protocol) {
	addr, err := utils.GetAddrFromHostname(host)
	if err != nil {
		fmt.Printf("error resolving address for host %s: %v\n", host, err)
		return
	}

	p.WriteChannel <- types.OutboundMessage{
		Data:      data,
		Recipient: addr,
		Protocol:  protocol,
	}
}

func (p *Peer) SendMessageToMembers(members *datastructures.SafeList[int], data []byte, protocol types.Protocol) {
	for _, member := range members.GetAll() {
		memberName, exists := p.PeerIdToName[member]
		if !exists {
			fmt.Println("Error: member ID not found in peerIdToName map:", member)
			continue
		}
		if memberName == p.Me {
			continue
		}
		go p.SendMessageToHost(memberName, data, protocol)
	}
}

// Heartbeat and monitoring
func (p *Peer) StartHeartbeatAndMonitoring() {
	p.waitForAllPeers()
	go p.ListenForUDPMessages()
	go p.startHeartbeat()
	go p.startMonitoring()
}

func (p *Peer) waitForAllPeers() {
	for len(p.Members.GetAll()) < len(p.Peers) {
		time.Sleep(1 * time.Second)
	}
}

func (p *Peer) startHeartbeat() {
	heartbeatData := types.EncodeIntegersToByteArray(int(types.HEARTBEAT))
	p.PeriodicSendBroadcastMessage(heartbeatData, heartbeatInterval)
}

func (p *Peer) PeriodicSendBroadcastMessage(data []byte, t float64) {
	ticker := time.NewTicker(time.Duration(int64(t * 1e9)))
	defer ticker.Stop()

	for range ticker.C {
		if err := p.BroadcastMessage(data); err != nil {
			fmt.Println("Error broadcasting message:", err)
		}
	}
}

func (p *Peer) BroadcastMessage(data []byte) error {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   net.IPv4bcast,
		Port: udpPort,
	})
	if err != nil {
		return fmt.Errorf("error creating UDP connection: %v", err)
	}
	defer conn.Close()

	if _, err = conn.Write(data); err != nil {
		return fmt.Errorf("error sending broadcast message: %v", err)
	}
	return nil
}

func (p *Peer) startMonitoring() {
	p.MonitorHeartBeats(monitorInterval)
}

func (p *Peer) MonitorHeartBeats(t float64) {
	ticker := time.NewTicker(time.Duration(int64(t * 1e9)))
	defer ticker.Stop()

	for range ticker.C {
		p.checkHeartbeats()
		p.resetHeartbeats()
	}
}

func (p *Peer) checkHeartbeats() {
	membersList := p.Members.GetAll()
	for _, member := range membersList {
		if p.PeerIdToName[member] == p.Me {
			continue
		}
		if beat, ok := p.Beats.Load(p.PeerIdToName[member]); !ok || !beat.(bool) {
			p.HandleFailedPeer(member)
		}
	}
}

func (p *Peer) resetHeartbeats() {
	for _, member := range p.Members.GetAll() {
		p.Beats.Store(p.PeerIdToName[member], false)
	}
}

// Failure handling
func (p *Peer) HandleFailedPeer(failedPeerId int) {
	failedPeerName := p.PeerIdToName[failedPeerId]

	if failedPeerName == p.Leader.Get() {
		p.logPeerFailure(failedPeerId, true)
	} else {
		p.logPeerFailure(failedPeerId, false)
	}

	if failedPeerName == p.Leader.Get() {
		p.handleLeaderFailure(failedPeerId)
	} else if p.isLeader() {
		p.handleMemberFailure(failedPeerId)
	}
}

func (p *Peer) handleLeaderFailure(failedPeerId int) {
	p.ElectNewLeader(failedPeerId)
}

func (p *Peer) handleMemberFailure(failedPeerId int) {
	p.InitiatePeerRemoval(failedPeerId)
}

func (p *Peer) ElectNewLeader(failedLeaderId int) {
	p.Members.Sort(func(a, b int) bool { return a < b })
	if newLeaderId, ok := p.Members.Get(1); ok {
		p.Leader.Set(p.PeerIdToName[newLeaderId])
	}
	if p.Leader.Get() == p.Me {
		p.handleNewLeadershipRole(failedLeaderId)
	}
}

func (p *Peer) handleNewLeadershipRole(failedLeaderId int) {
	p.AliveMembers.Replace(p.Members.GetAll())
	p.AliveMembers.RemoveByValue(failedLeaderId)

	// Send NEWLEADER message
	p.RequestId.Set(p.RequestId.Get() + 1)
	encodedData := types.EncodeIntegersToByteArray(int(types.NEWLEADER), p.RequestId.Get(), p.ViewId.Get(), int(types.PENDING))
	go p.SendMessageToMembers(p.AliveMembers, encodedData, types.TCP)

	// Create removal request
	p.RequestId.Set(p.RequestId.Get() + 1)
	p.PendingOps.Add(types.RequestMessage{
		ViewId:    p.ViewId.Get(),
		RequestId: p.RequestId.Get(),
		Operation: types.DEL,
		PeerId:    failedLeaderId,
	})
}

func (p *Peer) InitiatePeerRemoval(peerId int) {
	if _, ok := p.PendingOps.GetByPeerIdAndOperation(peerId, types.DEL); ok {
		return
	}

	p.RequestId.Set(p.RequestId.Get() + 1)
	requestMessage := types.RequestMessage{
		RequestId: p.RequestId.Get(),
		ViewId:    p.ViewId.Get(),
		Operation: types.DEL,
		PeerId:    peerId,
	}

	p.PendingOps.Add(requestMessage)
	encodedData := types.EncodeIntegersToByteArray(
		int(types.REQ),
		requestMessage.RequestId,
		requestMessage.ViewId,
		int(requestMessage.Operation),
		requestMessage.PeerId,
	)

	p.AliveMembers.Replace(p.Members.GetAll())
	p.AliveMembers.RemoveByValue(peerId)
	go p.SendMessageToMembers(p.AliveMembers, encodedData, types.TCP)

	if len(p.Members.GetAll()) == 2 {
		p.handleLastMemberRemoval(peerId)
	}
}

func (p *Peer) handleLastMemberRemoval(peerId int) {
	p.ViewId.Set(p.ViewId.Get() + 1)
	p.Members.RemoveByValue(peerId)
	p.AliveMembers.RemoveByValue(peerId)
	p.logMembershipChange()
}

// Test simulation
func (p *Peer) SimulateTestCase4() {
	go func() {
		p.waitForAllPeers()
		p.simulateRemovalAndCrash()
	}()
}

func (p *Peer) simulateRemovalAndCrash() {
	// Create removal request
	p.RequestId.Set(p.RequestId.Get() + 1)
	requestMessage := types.RequestMessage{
		ViewId:    p.ViewId.Get(),
		RequestId: p.RequestId.Get(),
		Operation: types.DEL,
		PeerId:    p.PeerNameToId[p.Peers[4]],
	}

	// Create target list excluding peers 2 and 5
	target := p.createTargetList()

	// Send messages and crash
	p.Sent.Set(0)
	go p.sendAndCrash(target, requestMessage)
}

func (p *Peer) createTargetList() *datastructures.SafeList[int] {
	target := &datastructures.SafeList[int]{}
	for _, member := range p.Members.GetAll() {
		if member != p.PeerNameToId[p.Peers[1]] && member != p.PeerNameToId[p.Peers[4]] {
			target.Add(member)
		}
	}
	return target
}

func (p *Peer) sendAndCrash(target *datastructures.SafeList[int], requestMessage types.RequestMessage) {
	encodedData := types.EncodeIntegersToByteArray(
		int(types.REQ),
		requestMessage.RequestId,
		requestMessage.ViewId,
		int(requestMessage.Operation),
		requestMessage.PeerId,
	)
	p.SendMessageToMembers(target, encodedData, types.TCP)

	// Wait until we've sent to all required peers
	for p.Sent.Get() < len(p.Peers)-3 {
		time.Sleep(100 * time.Millisecond)
	}

	// Log crash message and exit
	fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, message:\"crashing\"}\n",
		p.PeerNameToId[p.Me], p.ViewId.Get(), p.PeerNameToId[p.Leader.Get()])
	os.Exit(1)
}

// Utility functions
func (p *Peer) logMembershipChange() {
	fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, memb_list: [<%s>]}\n",
		p.PeerNameToId[p.Me],
		p.ViewId.Get(),
		p.PeerNameToId[p.Leader.Get()],
		strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.Members.GetAll())), ","), "[]"))
}

func (p *Peer) logPeerFailure(peerId int, isLeader bool) {
	leaderStr := ""
	if isLeader {
		leaderStr = " (leader)"
	}
	fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, message:\"peer %d%s unreachable\"}\n",
		p.PeerNameToId[p.Me],
		p.ViewId.Get(),
		p.PeerNameToId[p.Leader.Get()],
		peerId,
		leaderStr)
}
