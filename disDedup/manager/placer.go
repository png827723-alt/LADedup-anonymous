package manager

import (
	"dedup-system/config"
	"math/rand"
	"sync"
	"time"
)

type Placer struct {
	strategy string
	nodes    []config.NodeEndpoint

	mu  sync.Mutex
	idx int
}

func NewPlacer(strategy string, nodes []config.NodeEndpoint) *Placer {
	if strategy == "" {
		strategy = "round_robin"
	}
	rand.Seed(time.Now().UnixNano())
	return &Placer{strategy: strategy, nodes: nodes}
}

func (p *Placer) Next() config.NodeEndpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.nodes) == 0 {
		return config.NodeEndpoint{}
	}
	if p.strategy == "random" {
		return p.nodes[rand.Intn(len(p.nodes))]
	}
	// round robin
	ret := p.nodes[p.idx%len(p.nodes)]
	p.idx++
	return ret
}
