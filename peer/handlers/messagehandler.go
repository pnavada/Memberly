package handlers

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"peer/network"
	"peer/types"
	"peer/utils"
)

type MessageHandler struct {
	peer *network.Peer
	mu   sync.Mutex
}

func NewMessageHandler(p *network.Peer) *MessageHandler {
	return &MessageHandler{
		peer: p,
		mu:   sync.Mutex{},
	}
}

func (mh *MessageHandler) HandleMessages(countSent bool) {
	for {
		select {
		case inboundMessage := <-mh.peer.ReadChannel:
			mh.processInboundMessage(inboundMessage, countSent)
		case outboundMessage := <-mh.peer.WriteChannel:
			mh.sendMessage(outboundMessage)
		}
	}
}

func (mh *MessageHandler) processInboundMessage(message types.InboundMessage, countSent bool) {
	data, err := types.DecodeByteArrayToIntegers(message.Data)
	if err != nil {
		fmt.Println("Error decoding byte array to integers:", err)
		return
	}

	sender, err := utils.GetHostnameFromAddr(message.Sender)
	if err != nil {
		fmt.Println("Error getting hostname from address:", err)
		return
	}

	sender = utils.CleanHostname(sender)
	mh.handleMessage(types.MessageType(data[0]), data[1:], sender)
	if countSent {
		mh.peer.Sent.Set(mh.peer.Sent.Get() + 1)
	}
}

func (mh *MessageHandler) handleMessage(msgType types.MessageType, data []int, sender string) {
	switch msgType {
	case types.REQ:
		mh.handleReqMessage(data, sender)
	case types.OK:
		mh.handleOkMessage(data, sender)
	case types.HEARTBEAT:
		mh.handleHeartbeatMessage(sender)
	case types.NEWLEADER:
		mh.handleNewLeaderMessage(data, sender)
	case types.JOIN:
		mh.handleJoinMessage(sender)
	case types.NEWVIEW:
		mh.handleNewViewMessage(data)
	case types.PENDINGRESPONSE:
		go mh.handlePendingResponseMessage(data, sender)
	}
}

func (mh *MessageHandler) handleReqMessage(data []int, sender string) {
	requestMessage := types.RequestMessage{
		RequestId: data[0],
		ViewId:    data[1],
		Operation: types.OperationType(data[2]),
		PeerId:    data[3],
	}

	mh.peer.PendingOps.Add(requestMessage)
	okMessage := types.OKMessage{
		RequestId: data[0],
		ViewId:    data[1],
	}
	encodedData := types.EncodeIntegersToByteArray(int(types.OK), okMessage.RequestId, okMessage.ViewId)
	go mh.peer.SendMessageToHost(sender, encodedData, types.TCP)
}

func (mh *MessageHandler) handleOkMessage(data []int, sender string) {
	if mh.peer.Me != mh.peer.Leader.Get() {
		return
	}

	key := fmt.Sprintf("%d-%d", data[1], data[0])
	if count, ok := mh.peer.AckCount.Load(key); ok {
		mh.peer.AckCount.Store(key, count.(int)+1)
	} else {
		mh.peer.AckCount.Store(key, 1)
	}

	mh.checkAndProcessAcks(key, data[1], data[0])
}

func (mh *MessageHandler) checkAndProcessAcks(key string, viewId, requestId int) {
	op, ok := mh.peer.PendingOps.GetByViewAndRequestId(viewId, requestId)
	if !ok {
		return
	}

	rcount := len(mh.peer.AliveMembers.GetAll()) - 1
	if count, ok := mh.peer.AckCount.Load(key); ok && count.(int) == rcount {
		mh.peer.ViewId.Set(mh.peer.ViewId.Get() + 1)
		if op.Operation == types.ADD {
			mh.peer.Members.Add(op.PeerId)
			mh.peer.AliveMembers.Add(op.PeerId)
		} else if op.Operation == types.DEL {
			mh.peer.Members.RemoveByValue(op.PeerId)
		}

		mh.logMembershipChange()
		mh.broadcastNewView()
		mh.peer.PendingOps.RemoveByViewAndRequestId(viewId, requestId)
		mh.peer.OpsStatus.Store(key, true)
	}
}

func (mh *MessageHandler) handleHeartbeatMessage(sender string) {
	if mh.peer.Members.Contains(mh.peer.PeerNameToId[sender]) {
		mh.peer.Beats.Store(sender, true)
	}
}

func (mh *MessageHandler) handleNewLeaderMessage(data []int, sender string) {
	mh.peer.Leader.Set(sender)
	var pendingResponse []int
	pendingResponse = append(pendingResponse, int(types.PENDINGRESPONSE))

	if len(mh.peer.PendingOps.GetAll()) == 0 {
		pendingResponse = append(pendingResponse, data[0], data[1], int(types.NOTHING), -1)
	} else {
		op, _ := mh.peer.PendingOps.Get(0)
		pendingResponse = append(pendingResponse, data[0], data[1], int(op.Operation), op.PeerId)
		mh.peer.PendingOps.Replace([]types.RequestMessage{})
	}

	encodedData := types.EncodeIntegersToByteArray(pendingResponse...)
	go mh.peer.SendMessageToHost(sender, encodedData, types.TCP)
}

func (mh *MessageHandler) handleJoinMessage(sender string) {
	if len(mh.peer.Members.GetAll()) == 1 {
		mh.peer.Members.Add(mh.peer.PeerNameToId[sender])
		mh.peer.AliveMembers.Add(mh.peer.PeerNameToId[sender])
		mh.peer.ViewId.Set(mh.peer.ViewId.Get() + 1)
		mh.logMembershipChange()
		mh.broadcastNewView()
	} else {
		mh.peer.RequestId.Set(mh.peer.RequestId.Get() + 1)
		requestMessage := types.RequestMessage{
			RequestId: mh.peer.RequestId.Get(),
			ViewId:    mh.peer.ViewId.Get(),
			Operation: types.ADD,
			PeerId:    mh.peer.PeerNameToId[sender],
		}
		encodedData := types.EncodeIntegersToByteArray(
			int(types.REQ),
			requestMessage.RequestId,
			requestMessage.ViewId,
			int(requestMessage.Operation),
			requestMessage.PeerId,
		)
		go mh.peer.SendMessageToMembers(mh.peer.AliveMembers, encodedData, types.TCP)
		mh.peer.PendingOps.Add(requestMessage)
	}
}

func (mh *MessageHandler) handleNewViewMessage(data []int) {
	mh.peer.ViewId.Set(data[0])
	membersList := data[1:]
	mh.peer.Members.Replace(membersList)
	mh.peer.PendingOps.Remove(0)
	mh.logMembershipChange()
}

func (mh *MessageHandler) handlePendingResponseMessage(data []int, sender string) {
	requestMessage := types.RequestMessage{
		ViewId:    data[1],
		RequestId: data[0],
		Operation: types.OperationType(data[2]),
		PeerId:    data[3],
	}

	// Only add to pendingOps if not already present and operation is not NOTHING
	_, ok := mh.peer.PendingOps.GetByViewAndRequestId(requestMessage.ViewId, requestMessage.RequestId)
	if !ok && requestMessage.Operation != types.NOTHING {
		mh.peer.PendingOps.Insert(0, requestMessage)
	}

	key := fmt.Sprintf("%d-%d", data[1], data[0])
	if count, ok := mh.peer.AckCount.Load(key); ok {
		mh.peer.AckCount.Store(key, count.(int)+1)
	} else {
		mh.peer.AckCount.Store(key, 1)
	}

	rcount := len(mh.peer.AliveMembers.GetAll()) - 1

	if count, ok := mh.peer.AckCount.Load(key); ok && count.(int) == rcount {
		// Process all pending operations
		for _, op := range mh.peer.PendingOps.GetAll() {
			encodedData := types.EncodeIntegersToByteArray(
				int(types.REQ),
				op.RequestId,
				op.ViewId,
				int(op.Operation),
				op.PeerId,
			)
			if op.Operation == types.DEL {
				mh.peer.AliveMembers.RemoveByValue(op.PeerId)
			}
			mh.peer.SendMessageToMembers(mh.peer.AliveMembers, encodedData, types.TCP)
			key := fmt.Sprintf("%d-%d", op.ViewId, op.RequestId)
			mh.peer.AckCount.Store(key, 0)

			for {
				done, ok := mh.peer.OpsStatus.Load(key)
				if ok && done.(bool) {
					break
				}
			}
		}
	}
}

func (mh *MessageHandler) sendMessage(outboundMessage types.OutboundMessage) {
	var conn interface{}
	var err error

	switch outboundMessage.Protocol {
	case types.TCP:
		conn, err = mh.peer.TCPEgress.Get(outboundMessage.Recipient)
	case types.UDP:
		conn, err = mh.peer.UDPEgress.Get(outboundMessage.Recipient)
	}

	if err != nil {
		fmt.Printf("Error getting %v connection: %v\n", outboundMessage.Protocol, err)
		return
	}

	switch outboundMessage.Protocol {
	case types.TCP:
		tcpConn := conn.(net.Conn)
		_, err = tcpConn.Write(outboundMessage.Data)
	case types.UDP:
		udpConn := conn.(*net.UDPConn)
		_, err = udpConn.Write(outboundMessage.Data)
	}

	if err != nil {
		fmt.Printf("Error sending %v message: %v\n", outboundMessage.Protocol, err)
	}
}

func (mh *MessageHandler) logMembershipChange() {
	utils.PrintToStderr(fmt.Sprintf("{peer_id:%d, view_id: %d, leader: %d, memb_list: [<%s>]}",
		mh.peer.PeerNameToId[mh.peer.Me],
		mh.peer.ViewId.Get(),
		mh.peer.PeerNameToId[mh.peer.Leader.Get()],
		strings.Trim(strings.Join(strings.Fields(fmt.Sprint(mh.peer.Members.GetAll())), ","), "[]")))
}

func (mh *MessageHandler) broadcastNewView() {
	encodedData := types.EncodeIntegersToByteArray(
		append([]int{int(types.NEWVIEW), mh.peer.ViewId.Get()},
			mh.peer.Members.GetAll()...)...)
	go mh.peer.SendMessageToMembers(mh.peer.AliveMembers, encodedData, types.TCP)
}
