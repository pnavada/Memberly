package main

import (
	"fmt"
	"os"
	"time"

	"peer/config"
	"peer/handlers"
	"peer/network"
	"peer/utils"
)

func main() {

	cfg := config.ParseFlags()

	time.Sleep(time.Duration(cfg.InitialDelay) * time.Second)

	me, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting hostname:", err)
		os.Exit(1)
	}

	peers, err := utils.GetPeers(cfg.HostsFile)
	if err != nil {
		fmt.Println("Error reading peers:", err)
		os.Exit(1)
	}

	peerNameToId := utils.GetPeerNameToIdMapping(peers)
	peerIdToName := utils.GetPeerIdToNameMapping(peers)

	leaderName, err := utils.GetLeader(peers)
	if err != nil {
		fmt.Println("Error determining leader:", err)
		os.Exit(1)
	}

	p := network.NewPeer(me, leaderName, peerNameToId, peerIdToName, peers)

	if cfg.CrashDelay > 0 {
		go func() {
			time.Sleep(time.Duration(cfg.CrashDelay) * time.Second)
			fmt.Printf("{peer_id:%d, view_id: %d, leader: %d, message:\"crashing\"}\n",
				p.PeerNameToId[me], p.ViewId.Get(), p.PeerNameToId[p.Leader.Get()])
			os.Exit(1)
		}()
	}

	p.Start()

	// Handle Test Case 4 simulation
	if cfg.Crash {
		p.SimulateTestCase4()
	}

	messageHandler := handlers.NewMessageHandler(p)
	messageHandler.HandleMessages(cfg.Crash)
}
