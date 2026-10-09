package manager

import (
	"context"
	"dedup-system/config"
	"dedup-system/rpc"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodePool struct {
	cfg     config.Config
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]rpc.StorageNodeClient
}

func NewNodePool(cfg config.Config) *NodePool {
	return &NodePool{
		cfg:     cfg,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]rpc.StorageNodeClient),
	}
}

func (p *NodePool) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.cfg.RPCMaxMessageBytes),
			grpc.MaxCallSendMsgSize(p.cfg.RPCMaxMessageBytes),
		),
	}
}

func (p *NodePool) dial(endpoint config.NodeEndpoint) (rpc.StorageNodeClient, error) {
	if endpoint.ID == "" || endpoint.Addr == "" {
		return nil, fmt.Errorf("invalid endpoint: %+v", endpoint)
	}

	conn, err := grpc.Dial(endpoint.Addr, p.dialOptions()...)
	if err != nil {
		return nil, err
	}
	client := rpc.NewStorageNodeClient(conn)
	return client, nil
}

func endpointKey(endpoint config.NodeEndpoint) string {
	// Prefer address as the physical connection identity.
	if endpoint.Addr != "" {
		return endpoint.Addr
	}
	return endpoint.ID
}

func (p *NodePool) Get(endpoint config.NodeEndpoint) (rpc.StorageNodeClient, error) {
	if endpoint.Addr == "" {
		return nil, fmt.Errorf("invalid endpoint: %+v", endpoint)
	}
	key := endpointKey(endpoint)

	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[key]; ok {
		return c, nil
	}
	conn, err := grpc.Dial(endpoint.Addr, p.dialOptions()...)
	if err != nil {
		return nil, err
	}
	p.conns[key] = conn
	p.clients[key] = rpc.NewStorageNodeClient(conn)
	return p.clients[key], nil
}

func (p *NodePool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]rpc.StorageNodeClient)
}

func (p *NodePool) ctx() (context.Context, context.CancelFunc) {
	to := time.Duration(p.cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	return context.WithTimeout(context.Background(), to)
}

// Warmup pre-establishes reusable grpc connections and performs a Ping health check.
func (p *NodePool) Warmup(endpoints []config.NodeEndpoint) error {
	uniq := make(map[string]config.NodeEndpoint)
	for _, ep := range endpoints {
		if ep.Addr == "" {
			continue
		}
		uniq[endpointKey(ep)] = ep
	}

	var wg sync.WaitGroup
	errCh := make(chan string, len(uniq))

	for _, ep := range uniq {
		ep := ep
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := p.Get(ep)
			if err != nil {
				errCh <- fmt.Sprintf("%s(connect): %v", ep.ID, err)
				return
			}
			ctx, cancel := p.ctx()
			defer cancel()
			if _, err := client.Ping(ctx, &rpc.PingRequest{}); err != nil {
				errCh <- fmt.Sprintf("%s(ping): %v", ep.ID, err)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if len(errCh) == 0 {
		return nil
	}
	failed := make([]string, 0, len(errCh))
	for msg := range errCh {
		failed = append(failed, msg)
	}
	return fmt.Errorf("warmup failed on nodes: %s", strings.Join(failed, "; "))
}
