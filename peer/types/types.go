package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Protocol int

const (
	TCP Protocol = iota
	UDP
)

type MessageType int

const (
	REQ MessageType = iota
	OK
	HEARTBEAT
	NEWLEADER
	JOIN
	NEWVIEW
	PENDINGRESPONSE
)

type OperationType int

const (
	ADD OperationType = iota
	DEL
	NOTHING
	PENDING
)

type InboundMessage struct {
	Data   []byte
	Sender net.Addr
}

type OutboundMessage struct {
	Data      []byte
	Recipient net.Addr
	Protocol  Protocol
}

type RequestMessage struct {
	ViewId    int
	RequestId int
	Operation OperationType
	PeerId    int
}

type OKMessage struct {
	RequestId int
	ViewId    int
}

type NewViewMessage struct {
	ViewId  int
	Members []int
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
