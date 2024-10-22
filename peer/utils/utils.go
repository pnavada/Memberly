package utils

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

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

func GetLeader(peers []string) (string, error) {
	if len(peers) == 0 {
		return "", fmt.Errorf("no peers available")
	}
	return peers[0], nil
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

func RemoveSelf(peers []string, self string) ([]string, error) {
	var peersWithoutSelf []string
	for _, peer := range peers {
		if peer != self {
			peersWithoutSelf = append(peersWithoutSelf, peer)
		}
	}
	return peersWithoutSelf, nil
}
