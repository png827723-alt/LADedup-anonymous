#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DISDEDUP_DIR="${REPO_ROOT}/disDedup"
METHOD_SCRIPT="${DISDEDUP_DIR}/scripts/case1_cluster/run_case1_cluster.py"
GENERATED_DIR="${DISDEDUP_DIR}/scripts/data/input/generated"
REQUEST_DIR="${DISDEDUP_DIR}/scripts/data/input/request"
OUTPUT_BASE="${DISDEDUP_DIR}/scripts/data/output/manual_copy_case1_runs"

CAP_VALUES="${CAP_VALUES:-5,10,15,20,25,30}"
EDGE_COUNT="${EDGE_COUNT:-10}"
CHUNK_SIZE="${CHUNK_SIZE:-8192}"
STORAGE_GRANULARITY="${STORAGE_GRANULARITY:-block}"
CHUNKING_METHOD="${CHUNKING_METHOD:-fastcdc}"
DEDUP_MODE="${DEDUP_MODE:-synthetic_edge}"
K_VALUE="${K_VALUE:-1000}"
ALPHA_VALUE="${ALPHA_VALUE:-5}"
SERVER_NUM="${SERVER_NUM:-10}"
TIMESTAMP="${TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}"

usage() {
  cat <<'EOF'
Usage:
  run_case1_cluster_copy_generated.sh [--json <name>] [--cap-values <csv>]

Options:
  --json <name>        Run only one generated json file under scripts/data/input/generated
  --cap-values <csv>   Capacity percentages, default: 5,10,15,20,25,30
  -h, --help           Show this help

Environment overrides:
  CAP_VALUES, EDGE_COUNT, CHUNK_SIZE, STORAGE_GRANULARITY, CHUNKING_METHOD,
  DEDUP_MODE, K_VALUE, ALPHA_VALUE, SERVER_NUM
EOF
}

ONLY_JSON=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --json) ONLY_JSON="$2"; shift 2 ;;
    --cap-values) CAP_VALUES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

IFS=',' read -r -a CAPS <<< "${CAP_VALUES}"

map_input() {
  local json_name="$1"
  case "${json_name}" in
    fileInfo-exp_edgededup_wiki_cloud_full_file-zipf-s0p5-8KB.json|fileInfo-exp_edgededup_wiki_cloud_full_file-*.json)
      DATA_ROOT="/mnt/test/exp_edgededup"
      DATASET_CONTAINER_ROOT="/input/wiki_cloud_full_file"
      CLOUD_STORE_ROOT="/mnt/test/exp_edgededup/wiki_cloud_full_store"
      REQUEST_FILE="${REQUEST_DIR}/$(sed -E 's/^fileInfo-exp_edgededup_(.*)-8KB\.json$/\1.requests.txt/' <<< "${json_name}")"
      ;;
    fileInfo-sina_news_depth1_15days-zipf-s0p5-8KB.json|fileInfo-sina_news_depth1_15days-*.json)
      DATA_ROOT="/mnt/test/exp_edgededup"
      DATASET_CONTAINER_ROOT="/input/dedup_datasets/sina_news_depth1/15days"
      CLOUD_STORE_ROOT="/mnt/test/exp_edgededup/cloud_full_file_sina"
      REQUEST_FILE="${REQUEST_DIR}/$(sed -E 's/^fileInfo-sina_news_depth1_15days-zipf-(.*)-8KB\.json$/sina15days-zipf-\1.requests.txt/' <<< "${json_name}")"
      ;;
    *)
      echo "Unsupported json input: ${json_name}" >&2
      return 1
      ;;
  esac
}

cleanup_run() {
  local env_file="$1"
  docker compose --env-file "${env_file}" -f "${DISDEDUP_DIR}/docker-compose.yml" down >/dev/null 2>&1 || true
}

cleanup_edge_data() {
  local store_root="$1"
  docker run --rm -v "${store_root}:/store" alpine sh -lc '
    for edge_dir in /store/edge*; do
      [ -d "${edge_dir}" ] || continue
      edge_yaml="$(basename "${edge_dir}").yaml"
      for path in "${edge_dir}"/* "${edge_dir}"/.[!.]* "${edge_dir}"/..?*; do
        [ -e "${path}" ] || continue
        [ "$(basename "${path}")" = "${edge_yaml}" ] && continue
        rm -rf "${path}"
      done
    done
  '
}

run_one() {
  local input_json="$1"
  local cap_percent="$2"
  local json_name json_stem run_root store_root run_env
  local placement_host placement_container fileinfo_host fileinfo_container
  local request_host request_container cap_ratio log_file

  json_name="$(basename "${input_json}")"
  json_stem="${json_name%.json}"
  map_input "${json_name}"

  if [[ ! -f "${REQUEST_FILE}" ]]; then
    echo "Request file not found: ${REQUEST_FILE}" >&2
    return 1
  fi
  if [[ ! -d "${DATA_ROOT}" ]]; then
    echo "DATA_ROOT not found: ${DATA_ROOT}" >&2
    return 1
  fi
  if [[ ! -d "${CLOUD_STORE_ROOT}/cloud" ]]; then
    echo "Cloud store root missing cloud dir: ${CLOUD_STORE_ROOT}/cloud" >&2
    return 1
  fi
  if [[ ! -d "${CLOUD_STORE_ROOT}/cloud/storage/_fullfiles" ]]; then
    echo "Cloud store root missing _fullfiles dir: ${CLOUD_STORE_ROOT}/cloud/storage/_fullfiles" >&2
    return 1
  fi

  run_root="${OUTPUT_BASE}/${TIMESTAMP}/${json_stem}/cap${cap_percent}"
  store_root="${run_root}/store"
  run_env="${run_root}/compose.env"
  log_file="${run_root}/run.log"
  CURRENT_RUN_ENV="${run_env}"

  placement_host="${store_root}/manager/placement/${json_stem}.placement.cap${cap_percent}.json"
  placement_container="/data/placement/$(basename "${placement_host}")"
  fileinfo_host="${store_root}/manager/fileinfo/${json_name}"
  fileinfo_container="/data/fileinfo/${json_name}"
  request_host="${store_root}/manager/restore/$(basename "${REQUEST_FILE}")"
  request_container="/data/restore/$(basename "${REQUEST_FILE}")"
  cap_ratio="$(awk "BEGIN { printf \"%.4f\", ${cap_percent} / 100 }")"

  mkdir -p \
    "${run_root}" \
    "${store_root}/manager/placement" \
    "${store_root}/manager/fileinfo" \
    "${store_root}/manager/restore" \
    "${store_root}/output"

  cp "${DISDEDUP_DIR}/compose.paths.env" "${run_env}"
  sed -i "s|^DATA_ROOT=.*|DATA_ROOT=${DATA_ROOT}|" "${run_env}"
  sed -i "s|^STORE_ROOT=.*|STORE_ROOT=${store_root}|" "${run_env}"
  if grep -q '^CLOUD_STORE_ROOT=' "${run_env}"; then
    sed -i "s|^CLOUD_STORE_ROOT=.*|CLOUD_STORE_ROOT=${CLOUD_STORE_ROOT}|" "${run_env}"
  else
    printf '\nCLOUD_STORE_ROOT=%s\n' "${CLOUD_STORE_ROOT}" >> "${run_env}"
  fi

  cp "${input_json}" "${fileinfo_host}"
  cp "${REQUEST_FILE}" "${request_host}"

  {
    echo "[run] input_json=${input_json}"
    echo "[run] request_file=${REQUEST_FILE}"
    echo "[run] data_root=${DATA_ROOT}"
    echo "[run] dataset_container_root=${DATASET_CONTAINER_ROOT}"
    echo "[run] cloud_store_root=${CLOUD_STORE_ROOT}"
    echo "[run] cap_percent=${cap_percent}"

    python3 "${METHOD_SCRIPT}" \
      --input-json "${fileinfo_host}" \
      --output-json "${placement_host}" \
      --capacity-ratio "${cap_ratio}" \
      --k "${K_VALUE}" \
      --alpha "${ALPHA_VALUE}" \
      --server-num "${SERVER_NUM}"

    "${DISDEDUP_DIR}/scripts/config/sync_cluster_configs.sh" \
      --env-file "${run_env}" \
      --store-root "${store_root}" \
      --cloud-store-root "${CLOUD_STORE_ROOT}" \
      --edge-count "${EDGE_COUNT}" \
      --chunk-size "${CHUNK_SIZE}" \
      --storage-granularity "${STORAGE_GRANULARITY}" \
      --chunking-method "${CHUNKING_METHOD}" \
      --placement-json "${placement_container}" \
      --placement-strategy round_robin \
      --dedup-mode "${DEDUP_MODE}" \
      --synthetic-fileinfo-json "${fileinfo_container}" \
      --synthetic-dataset-prefix "${DATASET_CONTAINER_ROOT}" \
      --restore-cloud-full-threshold 1 \
      --restore-discard-output true \
      --load-previous-index false

    docker compose --env-file "${run_env}" -f "${DISDEDUP_DIR}/docker-compose.yml" \
      up -d --force-recreate \
      cloud edge1 edge2 edge3 edge4 edge5 edge6 edge7 edge8 edge9 edge10 manager

    cp "${input_json}" "${fileinfo_host}"

    docker compose --env-file "${run_env}" -f "${DISDEDUP_DIR}/docker-compose.yml" \
      run --rm manager manager \
      -config /cfg/manager.yaml \
      -placement-json "${placement_container}" \
      dedup "${DATASET_CONTAINER_ROOT}"

    docker compose --env-file "${run_env}" -f "${DISDEDUP_DIR}/docker-compose.yml" \
      run --rm manager manager \
      -config /cfg/manager.yaml \
      -placement-json "${placement_container}" \
      restore-batch "${request_container}"
  } 2>&1 | tee "${log_file}"

  cleanup_run "${run_env}"
  CURRENT_RUN_ENV=""

  cleanup_edge_data "${store_root}"

  echo "[done] ${json_stem} cap=${cap_percent} log=${log_file}"
}

main() {
  local input_json
  local -a inputs=()

  if [[ -n "${ONLY_JSON}" ]]; then
    inputs+=("${GENERATED_DIR}/${ONLY_JSON}")
  else
    while IFS= read -r input_json; do
      inputs+=("${input_json}")
    done < <(find "${GENERATED_DIR}" -maxdepth 1 -type f -name '*.json' | sort)
  fi

  if [[ "${#inputs[@]}" -eq 0 ]]; then
    echo "No input json files found under ${GENERATED_DIR}" >&2
    exit 1
  fi

  for input_json in "${inputs[@]}"; do
    if [[ ! -f "${input_json}" ]]; then
      echo "Input json not found: ${input_json}" >&2
      exit 1
    fi
    for cap in "${CAPS[@]}"; do
      run_one "${input_json}" "${cap}"
    done
  done
}

trap 'cleanup_run "${CURRENT_RUN_ENV:-/nonexistent}"' EXIT

main "$@"
