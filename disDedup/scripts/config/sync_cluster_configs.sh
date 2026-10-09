#!/usr/bin/env bash
set -euo pipefail

# Batch-generate manager/cloud/edge YAML configs under STORE_ROOT.
# This avoids editing each node config one-by-one.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DISDEDUP_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_ENV_FILE="${DISDEDUP_DIR}/compose.paths.env"

ENV_FILE="${DEFAULT_ENV_FILE}"
STORE_ROOT=""
CLOUD_STORE_ROOT=""
EDGE_COUNT=10

CLOUD_PORT=50051
EDGE_BASE_PORT=50052

CHUNKING_METHOD="fastcdc"
CHUNK_SIZE=8192
STORAGE_GRANULARITY="block"   # block | container

PLACEMENT_STRATEGY="round_robin" # round_robin | random
PLACEMENT_DIR="/data/placement"
PLACEMENT_JSON=""
CLOUD_CHUNK_REPLICATION="false"
LOAD_PREVIOUS_INDEX="false"
DEDUP_MODE="synthetic_edge"
SYNTHETIC_FILEINFO_JSON=""
SYNTHETIC_DATASET_PREFIX="/input/github_repo"

DEDUP_BATCH_SIZE=384
RESTORE_BATCH_SIZE=256
RESTORE_WINDOW_SIZE=4096
RESTORE_CLOUD_FULL_THRESHOLD=1
RPC_MAX_MESSAGE_BYTES=$((64 * 1024 * 1024))
RPC_TIMEOUT_MS=3000

CONTAINER_DATA_SIZE=4194304
META_RESERVED_SIZE=32768
SINGLE_FILE_MODE="false"
CACHE_SIZE=512
PERSIST_DATA="true"
RESTORE_DISCARD_OUTPUT="true"

usage() {
  cat <<'EOF'
Usage:
  sync_cluster_configs.sh [options]

Options:
  --env-file <path>             Path to compose env file (default: disDedup/compose.paths.env)
  --store-root <path>           Override STORE_ROOT directly
  --cloud-store-root <path>     Override CLOUD_STORE_ROOT directly
  --edge-count <N>              Number of edge nodes in manager.yaml (default: 10, max 10)
  --cloud-port <port>           Cloud RPC port (default: 50051)
  --edge-base-port <port>       edge1 port; edgeN = base + (N-1) (default: 50052)
  --chunking-method <m>         fastcdc | fixed (default: fastcdc)
  --chunk-size <bytes>          Chunk size (default: 8192)
  --storage-granularity <g>     block | container (default: block)
  --placement-strategy <s>      round_robin | random (default: round_robin)
  --placement-dir <path>        Manager placement dir in container (default: /data/placement)
  --placement-json <path>       Manager placement json file path in container (default: empty)
  --cloud-chunk-replication <bool>
                                true | false (default: false)
  --load-previous-index <bool>  true | false (default: false)
  --dedup-mode <mode>           real | synthetic_edge | synthetic_manager | synthetic(alias synthetic_edge)
  --synthetic-fileinfo-json <p> Manager fileInfo json in container for synthetic dedup
  --synthetic-dataset-prefix <p>
                                Synthetic dedup dataset prefix used for recipe naming
  --dedup-batch-size <N>        Manager dedup batch size (default: 384)
  --restore-batch-size <N>      Manager restore batch size (default: 256)
  --restore-window-size <N>     Manager restore window size (default: 4096)
  --restore-cloud-full-threshold <N>
                                Full-cloud restore trigger threshold (default: 1)
  --rpc-max-message-bytes <N>   gRPC max send/recv message bytes (default: 67108864)
  --rpc-timeout-ms <N>          gRPC request timeout in ms (default: 3000)
  --container-data-size <bytes> Container data size for nodes (default: 4194304)
  --meta-reserved-size <bytes>  Container meta reserve size (default: 32768)
  --single-file-mode <bool>     true | false (default: false)
  --cache-size <N>              Node cache size (default: 512)
  --persist-data <bool>         true | false (default: true)
  --restore-discard-output <bool>
                                true | false (default: true)
  -h, --help                    Show this help

Examples:
  ./scripts/config/sync_cluster_configs.sh --edge-count 10 --storage-granularity block
  ./scripts/config/sync_cluster_configs.sh --edge-count 6 --chunk-size 4096 --single-file-mode true
EOF
}

is_bool() {
  [[ "$1" == "true" || "$1" == "false" ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --store-root) STORE_ROOT="$2"; shift 2 ;;
    --cloud-store-root) CLOUD_STORE_ROOT="$2"; shift 2 ;;
    --edge-count) EDGE_COUNT="$2"; shift 2 ;;
    --cloud-port) CLOUD_PORT="$2"; shift 2 ;;
    --edge-base-port) EDGE_BASE_PORT="$2"; shift 2 ;;
    --chunking-method) CHUNKING_METHOD="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --storage-granularity) STORAGE_GRANULARITY="$2"; shift 2 ;;
    --placement-strategy) PLACEMENT_STRATEGY="$2"; shift 2 ;;
    --placement-dir) PLACEMENT_DIR="$2"; shift 2 ;;
    --placement-json) PLACEMENT_JSON="$2"; shift 2 ;;
    --cloud-chunk-replication) CLOUD_CHUNK_REPLICATION="$2"; shift 2 ;;
    --load-previous-index) LOAD_PREVIOUS_INDEX="$2"; shift 2 ;;
    --dedup-mode) DEDUP_MODE="$2"; shift 2 ;;
    --synthetic-fileinfo-json) SYNTHETIC_FILEINFO_JSON="$2"; shift 2 ;;
    --synthetic-dataset-prefix) SYNTHETIC_DATASET_PREFIX="$2"; shift 2 ;;
    --dedup-batch-size) DEDUP_BATCH_SIZE="$2"; shift 2 ;;
    --restore-batch-size) RESTORE_BATCH_SIZE="$2"; shift 2 ;;
    --restore-window-size) RESTORE_WINDOW_SIZE="$2"; shift 2 ;;
    --restore-cloud-full-threshold) RESTORE_CLOUD_FULL_THRESHOLD="$2"; shift 2 ;;
    --rpc-max-message-bytes) RPC_MAX_MESSAGE_BYTES="$2"; shift 2 ;;
    --rpc-timeout-ms) RPC_TIMEOUT_MS="$2"; shift 2 ;;
    --container-data-size) CONTAINER_DATA_SIZE="$2"; shift 2 ;;
    --meta-reserved-size) META_RESERVED_SIZE="$2"; shift 2 ;;
    --single-file-mode) SINGLE_FILE_MODE="$2"; shift 2 ;;
    --cache-size) CACHE_SIZE="$2"; shift 2 ;;
    --persist-data) PERSIST_DATA="$2"; shift 2 ;;
    --restore-discard-output) RESTORE_DISCARD_OUTPUT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${STORE_ROOT}" ]]; then
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "env file not found: ${ENV_FILE}" >&2
    exit 1
  fi
  # shellcheck source=/dev/null
  set -a
  source "${ENV_FILE}"
  set +a
  STORE_ROOT="${STORE_ROOT:-}"
  CLOUD_STORE_ROOT="${CLOUD_STORE_ROOT:-}"
fi

if [[ -z "${STORE_ROOT}" ]]; then
  echo "STORE_ROOT is empty. Pass --store-root or define STORE_ROOT in env file." >&2
  exit 1
fi

if [[ -z "${CLOUD_STORE_ROOT}" ]]; then
  CLOUD_STORE_ROOT="${STORE_ROOT}/cloud"
fi

if ! [[ "${EDGE_COUNT}" =~ ^[0-9]+$ ]] || (( EDGE_COUNT <= 0 || EDGE_COUNT > 10 )); then
  echo "--edge-count must be 1..10 (docker-compose currently defines edge1..edge10)." >&2
  exit 1
fi

if [[ "${STORAGE_GRANULARITY}" != "block" && "${STORAGE_GRANULARITY}" != "container" ]]; then
  echo "--storage-granularity must be block or container" >&2
  exit 1
fi

if [[ "${CHUNKING_METHOD}" != "fastcdc" && "${CHUNKING_METHOD}" != "fixed" ]]; then
  echo "--chunking-method must be fastcdc or fixed" >&2
  exit 1
fi

if [[ "${PLACEMENT_STRATEGY}" != "round_robin" && "${PLACEMENT_STRATEGY}" != "random" ]]; then
  echo "--placement-strategy must be round_robin or random" >&2
  exit 1
fi

if [[ "${DEDUP_MODE}" != "real" && "${DEDUP_MODE}" != "synthetic" && "${DEDUP_MODE}" != "synthetic_edge" && "${DEDUP_MODE}" != "synthetic_manager" ]]; then
  echo "--dedup-mode must be real, synthetic_edge, synthetic_manager, or synthetic" >&2
  exit 1
fi

for b in "${CLOUD_CHUNK_REPLICATION}" "${LOAD_PREVIOUS_INDEX}" "${SINGLE_FILE_MODE}" "${PERSIST_DATA}" "${RESTORE_DISCARD_OUTPUT}"; do
  if ! is_bool "${b}"; then
    echo "boolean options must be true/false, got: ${b}" >&2
    exit 1
  fi
done

if ! [[ "${RESTORE_CLOUD_FULL_THRESHOLD}" =~ ^[0-9]+$ ]] || (( RESTORE_CLOUD_FULL_THRESHOLD <= 0 )); then
  echo "--restore-cloud-full-threshold must be a positive integer" >&2
  exit 1
fi

mkdir -p "${STORE_ROOT}/manager" "${CLOUD_STORE_ROOT}/cloud"
for i in $(seq 1 "${EDGE_COUNT}"); do
  mkdir -p "${STORE_ROOT}/edge${i}"
done

MANAGER_YAML="${STORE_ROOT}/manager/manager.yaml"
CLOUD_YAML="${CLOUD_STORE_ROOT}/cloud/cloud.yaml"

cat > "${MANAGER_YAML}" <<EOF
recipe_path: "/data/recipe"
global_meta_path: "/data/global/global_index.json"
stats_path: "/data/stats"
load_previous_index: ${LOAD_PREVIOUS_INDEX}
placement_dir: "${PLACEMENT_DIR}"
placement_json: "${PLACEMENT_JSON}"
placement_strategy: ${PLACEMENT_STRATEGY}
cloud_chunk_replication: ${CLOUD_CHUNK_REPLICATION}
dedup_mode: "${DEDUP_MODE}"
synthetic_fileinfo_json: "${SYNTHETIC_FILEINFO_JSON}"
synthetic_dataset_prefix: "${SYNTHETIC_DATASET_PREFIX}"
restore_batch_size: ${RESTORE_BATCH_SIZE}
dedup_batch_size: ${DEDUP_BATCH_SIZE}
restore_window_size: ${RESTORE_WINDOW_SIZE}
restore_cloud_full_threshold: ${RESTORE_CLOUD_FULL_THRESHOLD}
restore_discard_output: ${RESTORE_DISCARD_OUTPUT}
rpc_max_message_bytes: ${RPC_MAX_MESSAGE_BYTES}
rpc_timeout_ms: ${RPC_TIMEOUT_MS}
chunking_method: ${CHUNKING_METHOD}
chunk_size: ${CHUNK_SIZE}
storage_granularity: ${STORAGE_GRANULARITY}

cloud_node:
  id: "cloud"
  addr: "cloud:${CLOUD_PORT}"

edge_nodes:
EOF

for i in $(seq 1 "${EDGE_COUNT}"); do
  p=$((EDGE_BASE_PORT + i - 1))
  cat >> "${MANAGER_YAML}" <<EOF
  - id: "edge${i}"
    addr: "edge${i}:${p}"
EOF
done

cat > "${CLOUD_YAML}" <<EOF
role: cloud
node_id: "cloud"
listen_addr: "0.0.0.0:${CLOUD_PORT}"
dedup_mode: "${DEDUP_MODE}"

storage_granularity: "${STORAGE_GRANULARITY}"
storage_path: "/data/storage"
meta_path: "/data/meta/index.json"

container_data_size: ${CONTAINER_DATA_SIZE}
meta_reserved_size: ${META_RESERVED_SIZE}
single_file_mode: ${SINGLE_FILE_MODE}
cache_size: ${CACHE_SIZE}
persist_data: ${PERSIST_DATA}
rpc_max_message_bytes: ${RPC_MAX_MESSAGE_BYTES}
rpc_timeout_ms: ${RPC_TIMEOUT_MS}
EOF

for i in $(seq 1 "${EDGE_COUNT}"); do
  p=$((EDGE_BASE_PORT + i - 1))
  cat > "${STORE_ROOT}/edge${i}/edge${i}.yaml" <<EOF
role: edge
node_id: "edge${i}"
listen_addr: "0.0.0.0:${p}"
dedup_mode: "${DEDUP_MODE}"

storage_granularity: "${STORAGE_GRANULARITY}"
storage_path: "/data/storage"
meta_path: "/data/meta/index.json"

container_data_size: ${CONTAINER_DATA_SIZE}
meta_reserved_size: ${META_RESERVED_SIZE}
single_file_mode: ${SINGLE_FILE_MODE}
cache_size: ${CACHE_SIZE}
persist_data: ${PERSIST_DATA}
rpc_max_message_bytes: ${RPC_MAX_MESSAGE_BYTES}
rpc_timeout_ms: ${RPC_TIMEOUT_MS}
EOF
done

echo "Generated configs under: ${STORE_ROOT}"
echo "  manager: ${MANAGER_YAML}"
echo "  cloud:   ${CLOUD_YAML}"
echo "  edges:   edge1..edge${EDGE_COUNT}"
echo
echo "Next step:"
echo "  cd ${DISDEDUP_DIR}"
echo "  docker compose --env-file ${ENV_FILE} up -d --force-recreate"
