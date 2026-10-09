package main

import (
	"dedup-system/config"
	"dedup-system/manager"
	"dedup-system/node"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	var cfgPath string
	var placementJSON string
	var placementDir string
	var placementStrategy string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config yaml")
	flag.StringVar(&placementJSON, "placement-json", "", "override placement json file path (chunk_hash -> edge_node_id)")
	flag.StringVar(&placementDir, "placement-dir", "", "override placement dir path")
	flag.StringVar(&placementStrategy, "placement-strategy", "", "override placement strategy when placement table is disabled: round_robin|random")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage:")
		fmt.Println("  manager -config manager.yaml dedup   <input_path>")
		fmt.Println("  manager -config manager.yaml dedup-synthetic <fileinfo_json> [dataset_prefix]")
		fmt.Println("  manager -config manager.yaml restore <recipe_name_or_original_path> [output_path]")
		fmt.Println("  manager -config manager.yaml restore-cloud <recipe_name_or_original_path> [output_path]")
		fmt.Println("  manager -config manager.yaml restore-exp <edge-all|cloud-full|hybrid> <recipe_name_or_original_path> [output_path] [edge_ratio]")
		fmt.Println("  manager -config manager.yaml restore-batch <request_file>")
		fmt.Println("  manager -config manager.yaml restore-cloud-batch <request_file>")
		fmt.Println("  manager -config manager.yaml restore-exp-batch <edge-all|cloud-full|hybrid> <request_file> [edge_ratio]")
		fmt.Println("  manager -config manager.yaml -placement-json <placement.json> restore-fileinfo-batch <fileinfo_json> <request_file> [dataset_prefix]")
		fmt.Println("  manager -config manager.yaml cloud-sync <input_path>")
		fmt.Println()
		fmt.Println("Optional overrides:")
		fmt.Println("  -placement-json <file>      Use explicit placement JSON file (highest priority)")
		fmt.Println("  -placement-dir <dir>        Use placement tables from directory")
		fmt.Println("  -placement-strategy <name>  round_robin | random (used when placement table disabled)")
		os.Exit(2)
	}

	cfg, err := config.LoadConfigFromFile(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if cfg.Role == "" {
		cfg.Role = "manager"
	}
	if placementJSON != "" {
		cfg.PlacementJSON = placementJSON
	}
	if placementDir != "" {
		cfg.PlacementDir = placementDir
	}
	if placementStrategy != "" {
		cfg.PlacementStrategy = placementStrategy
	}
	// defaults
	node.DefaultRPCSettings(&cfg)
	if cfg.DistributedEnabled == false {
		cfg.DistributedEnabled = true
	}

	switch args[0] {
	case "dedup":
		if len(args) < 2 {
			log.Fatalf("dedup requires <input_path>")
		}
		dedupMode := cfg.DedupMode
		if dedupMode == "" {
			dedupMode = "synthetic_edge"
		}
		switch dedupMode {
		case "real":
			if err := manager.RunDedupDistributed(cfg, args[1]); err != nil {
				log.Fatalf("dedup failed: %v", err)
			}
		case "synthetic", "synthetic_edge":
			fileInfoJSON := cfg.SyntheticFileInfoJSON
			if fileInfoJSON == "" {
				log.Fatalf("dedup synthetic mode requires synthetic_fileinfo_json in manager config")
			}
			datasetPrefix := cfg.SyntheticDatasetPrefix
			if datasetPrefix == "" {
				datasetPrefix = args[1]
			}
			if err := manager.RunSyntheticDedupDistributedEdge(cfg, fileInfoJSON, datasetPrefix); err != nil {
				log.Fatalf("dedup failed: %v", err)
			}
		case "synthetic_manager":
			fileInfoJSON := cfg.SyntheticFileInfoJSON
			if fileInfoJSON == "" {
				log.Fatalf("dedup synthetic_manager mode requires synthetic_fileinfo_json in manager config")
			}
			datasetPrefix := cfg.SyntheticDatasetPrefix
			if datasetPrefix == "" {
				datasetPrefix = args[1]
			}
			if err := manager.RunSyntheticDedupDistributedManager(cfg, fileInfoJSON, datasetPrefix); err != nil {
				log.Fatalf("dedup failed: %v", err)
			}
		default:
			log.Fatalf("unknown dedup_mode %q in manager config; expected real, synthetic_edge, synthetic_manager, or synthetic", dedupMode)
		}
	case "dedup-synthetic":
		if len(args) < 2 {
			log.Fatalf("dedup-synthetic requires <fileinfo_json> [dataset_prefix]")
		}
		datasetPrefix := "/input/github_repo"
		if len(args) >= 3 {
			datasetPrefix = args[2]
		}
		if err := manager.RunSyntheticDedupDistributed(cfg, args[1], datasetPrefix); err != nil {
			log.Fatalf("dedup-synthetic failed: %v", err)
		}
	case "restore":
		if len(args) < 2 || (len(args) < 3 && !cfg.RestoreDiscardOutput) {
			log.Fatalf("restore requires <recipe_name_or_original_path> <output_path> unless restore_discard_output=true")
		}
		outputPath := ""
		if len(args) >= 3 {
			outputPath = args[2]
		}
		if err := manager.RunRestoreDistributed(cfg, args[1], outputPath); err != nil {
			log.Fatalf("restore failed: %v", err)
		}
	case "restore-cloud":
		if len(args) < 2 || (len(args) < 3 && !cfg.RestoreDiscardOutput) {
			log.Fatalf("restore-cloud requires <recipe_name_or_original_path> <output_path> unless restore_discard_output=true")
		}
		outputPath := ""
		if len(args) >= 3 {
			outputPath = args[2]
		}
		if err := manager.RunRestoreCloudOnly(cfg, args[1], outputPath); err != nil {
			log.Fatalf("restore-cloud failed: %v", err)
		}
	case "restore-exp":
		if len(args) < 3 {
			log.Fatalf("restore-exp requires <edge-all|cloud-full|hybrid> <recipe_name_or_original_path> [output_path] [edge_ratio]")
		}
		mode := args[1]
		outputPath := ""
		edgeRatio := 0.0
		if mode == manager.RestoreExperimentHybrid {
			switch {
			case !cfg.RestoreDiscardOutput && len(args) < 5:
				log.Fatalf("restore-exp hybrid requires <output_path> <edge_ratio> unless restore_discard_output=true")
			case cfg.RestoreDiscardOutput && len(args) == 4:
				v, err := strconv.ParseFloat(args[3], 64)
				if err != nil {
					log.Fatalf("invalid edge_ratio %q: %v", args[3], err)
				}
				edgeRatio = v
			case len(args) >= 5:
				outputPath = args[3]
				v, err := strconv.ParseFloat(args[4], 64)
				if err != nil {
					log.Fatalf("invalid edge_ratio %q: %v", args[4], err)
				}
				edgeRatio = v
			default:
				log.Fatalf("restore-exp hybrid requires <edge_ratio>")
			}
		} else {
			if len(args) < 4 && !cfg.RestoreDiscardOutput {
				log.Fatalf("restore-exp %s requires <output_path> unless restore_discard_output=true", mode)
			}
			if len(args) >= 4 {
				outputPath = args[3]
			}
		}
		if err := manager.RunRestoreExperiment(cfg, mode, args[2], outputPath, edgeRatio); err != nil {
			log.Fatalf("restore-exp failed: %v", err)
		}
	case "restore-batch":
		if len(args) < 2 {
			log.Fatalf("restore-batch requires <request_file>")
		}
		requests, err := manager.LoadRestoreRequests(args[1])
		if err != nil {
			log.Fatalf("load restore requests failed: %v", err)
		}
		if err := manager.RunRestoreDistributedBatch(cfg, requests); err != nil {
			log.Fatalf("restore-batch failed: %v", err)
		}
	case "restore-cloud-batch":
		if len(args) < 2 {
			log.Fatalf("restore-cloud-batch requires <request_file>")
		}
		requests, err := manager.LoadRestoreRequests(args[1])
		if err != nil {
			log.Fatalf("load restore requests failed: %v", err)
		}
		if err := manager.RunRestoreCloudOnlyBatch(cfg, requests); err != nil {
			log.Fatalf("restore-cloud-batch failed: %v", err)
		}
	case "restore-exp-batch":
		if len(args) < 3 {
			log.Fatalf("restore-exp-batch requires <edge-all|cloud-full|hybrid> <request_file> [edge_ratio]")
		}
		mode := args[1]
		requests, err := manager.LoadRestoreRequests(args[2])
		if err != nil {
			log.Fatalf("load restore requests failed: %v", err)
		}
		edgeRatio := 0.0
		if mode == manager.RestoreExperimentHybrid {
			if len(args) < 4 {
				log.Fatalf("restore-exp-batch hybrid requires <edge_ratio>")
			}
			v, err := strconv.ParseFloat(args[3], 64)
			if err != nil {
				log.Fatalf("invalid edge_ratio %q: %v", args[3], err)
			}
			edgeRatio = v
		}
		if err := manager.RunRestoreExperimentBatch(cfg, mode, requests, edgeRatio); err != nil {
			log.Fatalf("restore-exp-batch failed: %v", err)
		}
	case "restore-fileinfo-batch":
		if len(args) < 3 {
			log.Fatalf("restore-fileinfo-batch requires <fileinfo_json> <request_file> [dataset_prefix]")
		}
		requests, err := manager.LoadRestoreRequests(args[2])
		if err != nil {
			log.Fatalf("load restore requests failed: %v", err)
		}
		datasetPrefix := cfg.SyntheticDatasetPrefix
		if len(args) >= 4 {
			datasetPrefix = args[3]
		}
		if err := manager.RunRestoreFromFileInfoBatch(cfg, args[1], requests, datasetPrefix); err != nil {
			log.Fatalf("restore-fileinfo-batch failed: %v", err)
		}
	case "cloud-sync":
		if len(args) < 2 {
			log.Fatalf("cloud-sync requires <input_path>")
		}
		if err := manager.RunCloudSync(cfg, args[1]); err != nil {
			log.Fatalf("cloud-sync failed: %v", err)
		}
	default:
		log.Fatalf("unknown command: %s", args[0])
	}
}
