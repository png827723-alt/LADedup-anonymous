package manager

import "dedup-system/config"

func allStorageEndpoints(cfg config.Config, includeCloud bool) []config.NodeEndpoint {
	endpoints := make([]config.NodeEndpoint, 0, len(cfg.EdgeNodes)+1)
	endpoints = append(endpoints, cfg.EdgeNodes...)
	if includeCloud && cfg.CloudNode.ID != "" && cfg.CloudNode.Addr != "" {
		endpoints = append(endpoints, cfg.CloudNode)
	}
	return endpoints
}
