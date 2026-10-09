package main

import (
	"dedup-system/config"
	"dedup-system/node"
	"dedup-system/rpc"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config yaml")
	flag.Parse()

	cfg, err := config.LoadConfigFromFile(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if cfg.Role == "" {
		cfg.Role = "edge"
	}
	node.DefaultRPCSettings(&cfg)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":50051"
	}

	srvImpl, err := node.NewStorageNode(cfg)
	if err != nil {
		log.Fatalf("init storage node: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.ListenAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.RPCMaxMessageBytes),
		grpc.MaxSendMsgSize(cfg.RPCMaxMessageBytes),
	)
	rpc.RegisterStorageNodeServer(grpcServer, srvImpl)

	log.Printf(
		"[StorageNode] role=%s node_id=%s listen=%s granularity=%s dedup_mode=%s storage_path=%s",
		cfg.Role,
		cfg.NodeID,
		cfg.ListenAddr,
		cfg.StorageGranularity,
		cfg.DedupMode,
		cfg.StoragePath,
	)

	// Graceful shutdown
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stopCh
		log.Printf("[StorageNode] shutting down...")
		_ = srvImpl.Close()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "serve error: %v\n", err)
		os.Exit(1)
	}
}
