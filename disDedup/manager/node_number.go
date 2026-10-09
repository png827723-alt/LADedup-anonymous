package manager

import "dedup-system/config"

const CloudNodeNumber = 0

func endpointNumber(nodes []config.NodeEndpoint, endpoint config.NodeEndpoint) int {
	for i, n := range nodes {
		if n.ID != "" && endpoint.ID != "" && n.ID == endpoint.ID {
			return i + 1
		}
		if n.Addr != "" && endpoint.Addr != "" && n.Addr == endpoint.Addr {
			return i + 1
		}
	}
	return 0
}

func findEndpointByNumber(nodes []config.NodeEndpoint, nodeNumber int) (config.NodeEndpoint, bool) {
	if nodeNumber <= 0 || nodeNumber > len(nodes) {
		return config.NodeEndpoint{}, false
	}
	return nodes[nodeNumber-1], true
}
