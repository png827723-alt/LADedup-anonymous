#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ENV_FILE="${ROOT_DIR}/compose.paths.env"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
MANAGER_CONFIG="/cfg/manager.yaml"
PLACEMENT_JSON=""
PLACEMENT_DIR=""
PLACEMENT_STRATEGY=""
INPUT_DIR=""
OUTPUT_DIR="/output/restored"
REQUEST_FILE_HOST=""
REQUEST_FILE_CONTAINER=""
INCLUDE_PATTERN="*"
EXCLUDE_PATTERN=""
MAX_FILES=0
RUN_RESTORE=0

FILEINFO_JSON=""
REQUEST_COUNT=0
REQUEST_MULTIPLIER=0
SAMPLE_WITH_REPLACEMENT=1
RANDOM_SEED=""
POISSON_RATE=0
ARRIVAL_FILE_HOST=""
VERIFY_EXISTS=1

usage() {
  cat <<'HELP'
Generate restore-batch request file and optionally run manager restore-batch.

Usage:
  batch_restore_from_dir.sh --input-dir <host_dir> [options]

Required:
  --input-dir <path>            Host input root.

Basic Options:
  --output-dir <path>           Output base path in manager container. Default: /output/restored
  --request-file-host <path>    Host path to write request file.
  --request-file-container <p>  Request file path used inside manager container.
  --include-pattern <glob>      Include files matching glob basename. Default: *
  --exclude-pattern <glob>      Exclude files matching glob basename.
  --max-files <N>               Max files in directory-scan mode (0 means no limit). Default: 0
  --env-file <path>             Compose env file. Default: disDedup/compose.paths.env
  --compose-file <path>         Compose file. Default: disDedup/docker-compose.yml
  --config <path>               Manager config path in container. Default: /cfg/manager.yaml
  --placement-json <path>       Override manager -placement-json in container.
  --placement-dir <path>        Override manager -placement-dir in container.
  --placement-strategy <name>   Override manager -placement-strategy (round_robin|random).
  --run                         Run docker compose manager restore-batch after generating request file

Heat Sampling Options (from mean_go_v1 fileInfo JSON):
  --fileinfo-json <path>        Enable heat-weighted random sampling mode.
  --request-count <N>           Number of restore requests to sample.
  --request-multiplier <N>      Auto request count = N * fileinfo files count.
                                Example: N=2 => requests=2*len(fileinfo.files)
                                In fileinfo mode, provide one of --request-count / --request-multiplier.
  --sample-with-replacement     Weighted sampling with replacement (default in fileinfo mode).
  --sample-without-replacement  Weighted sampling without replacement.
  --seed <N>                    Random seed for reproducible sampling.
  --skip-missing                Allow paths missing under --input-dir (default: filter missing files out).

Poisson Arrival Options:
  --poisson-rate <lambda>       Poisson arrival rate (req/s). If >0, generate arrival schedule.
  --arrival-file-host <path>    Host path for arrival schedule TSV.

  -h, --help                    Show help.

Modes:
  1) Default mode (no --fileinfo-json): scan --input-dir and emit requests in sorted order.
  2) FileInfo mode (--fileinfo-json): sample by files[].heat weights from JSON.

Output Files:
  - request file: <input_path>\t<output_path>
  - arrival file (if poisson-rate > 0): <arrival_sec>\t<input_path>\t<output_path>

Notes:
  1) If --run is set and request file is not under ${STORE_ROOT}/manager,
     script copies it to ${STORE_ROOT}/manager and uses /data/<basename> in container.
  2) --run submits one batch immediately; Poisson schedule is generated as a trace file only.
HELP
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-dir)
      INPUT_DIR="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --request-file-host)
      REQUEST_FILE_HOST="${2:-}"
      shift 2
      ;;
    --request-file-container)
      REQUEST_FILE_CONTAINER="${2:-}"
      shift 2
      ;;
    --include-pattern)
      INCLUDE_PATTERN="${2:-}"
      shift 2
      ;;
    --exclude-pattern)
      EXCLUDE_PATTERN="${2:-}"
      shift 2
      ;;
    --max-files)
      MAX_FILES="${2:-0}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --compose-file)
      COMPOSE_FILE="${2:-}"
      shift 2
      ;;
    --config)
      MANAGER_CONFIG="${2:-}"
      shift 2
      ;;
    --placement-json)
      PLACEMENT_JSON="${2:-}"
      shift 2
      ;;
    --placement-dir)
      PLACEMENT_DIR="${2:-}"
      shift 2
      ;;
    --placement-strategy)
      PLACEMENT_STRATEGY="${2:-}"
      shift 2
      ;;
    --fileinfo-json)
      FILEINFO_JSON="${2:-}"
      shift 2
      ;;
    --request-count)
      REQUEST_COUNT="${2:-0}"
      shift 2
      ;;
    --request-multiplier)
      REQUEST_MULTIPLIER="${2:-0}"
      shift 2
      ;;
    --sample-with-replacement)
      SAMPLE_WITH_REPLACEMENT=1
      shift
      ;;
    --sample-without-replacement)
      SAMPLE_WITH_REPLACEMENT=0
      shift
      ;;
    --seed)
      RANDOM_SEED="${2:-}"
      shift 2
      ;;
    --poisson-rate)
      POISSON_RATE="${2:-0}"
      shift 2
      ;;
    --arrival-file-host)
      ARRIVAL_FILE_HOST="${2:-}"
      shift 2
      ;;
    --skip-missing)
      VERIFY_EXISTS=0
      shift
      ;;
    --run)
      RUN_RESTORE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${INPUT_DIR}" ]]; then
  echo "ERROR: --input-dir is required." >&2
  usage
  exit 2
fi
if [[ -z "${OUTPUT_DIR}" || "${OUTPUT_DIR}" != /* ]]; then
  echo "ERROR: --output-dir must be a container path starting with '/' (got: ${OUTPUT_DIR})." >&2
  exit 2
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: env file not found: ${ENV_FILE}" >&2
  exit 2
fi
if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "ERROR: compose file not found: ${COMPOSE_FILE}" >&2
  exit 2
fi

read_env_value() {
  local key="$1"
  local line
  line="$(grep -E "^[[:space:]]*${key}=" "${ENV_FILE}" | tail -n 1 || true)"
  if [[ -z "${line}" ]]; then
    echo ""
    return 0
  fi
  line="${line#*=}"
  line="${line%$'\r'}"
  line="$(printf '%s' "${line}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ "${line}" == \"*\" && "${line}" == *\" && "${#line}" -ge 2 ]]; then
    line="${line:1:${#line}-2}"
  elif [[ "${line}" == \'*\' && "${line}" == *\' && "${#line}" -ge 2 ]]; then
    line="${line:1:${#line}-2}"
  fi
  echo "${line}"
}

STORE_ROOT="$(read_env_value "STORE_ROOT")"
DATA_ROOT="$(read_env_value "DATA_ROOT")"

if [[ -z "${STORE_ROOT:-}" ]]; then
  echo "ERROR: STORE_ROOT is empty in ${ENV_FILE}" >&2
  exit 2
fi
if [[ -z "${DATA_ROOT:-}" ]]; then
  echo "ERROR: DATA_ROOT is empty in ${ENV_FILE}" >&2
  exit 2
fi

INPUT_DIR_ABS="$(readlink -f "${INPUT_DIR}")"
DATA_ROOT_ABS="$(readlink -f "${DATA_ROOT}")"
if [[ ! -d "${INPUT_DIR_ABS}" ]]; then
  echo "ERROR: input dir not found: ${INPUT_DIR_ABS}" >&2
  exit 2
fi

if [[ -n "${FILEINFO_JSON}" ]]; then
  FILEINFO_JSON="$(readlink -f "${FILEINFO_JSON}")"
  if [[ ! -f "${FILEINFO_JSON}" ]]; then
    echo "ERROR: fileinfo json not found: ${FILEINFO_JSON}" >&2
    exit 2
  fi
  if ! [[ "${REQUEST_COUNT}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --request-count must be a non-negative integer." >&2
    exit 2
  fi
  if ! [[ "${REQUEST_MULTIPLIER}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --request-multiplier must be a non-negative integer." >&2
    exit 2
  fi
  if [[ "${REQUEST_COUNT}" -gt 0 && "${REQUEST_MULTIPLIER}" -gt 0 ]]; then
    echo "ERROR: use only one of --request-count or --request-multiplier." >&2
    exit 2
  fi
  if [[ "${REQUEST_COUNT}" -le 0 && "${REQUEST_MULTIPLIER}" -le 0 ]]; then
    echo "ERROR: in --fileinfo-json mode, set --request-count > 0 or --request-multiplier > 0." >&2
    exit 2
  fi
fi

if [[ -z "${REQUEST_FILE_HOST}" ]]; then
  timestamp="$(date +%Y%m%d_%H%M%S)"
  REQUEST_FILE_HOST="${STORE_ROOT}/manager/restore_requests_${timestamp}.txt"
fi

# If user passed a directory path, place a timestamped request file under it.
if [[ -d "${REQUEST_FILE_HOST}" ]]; then
  timestamp="$(date +%Y%m%d_%H%M%S)"
  REQUEST_FILE_HOST="${REQUEST_FILE_HOST%/}/restore_requests_${timestamp}.txt"
elif [[ "${REQUEST_FILE_HOST}" == */ ]]; then
  mkdir -p "${REQUEST_FILE_HOST%/}"
  timestamp="$(date +%Y%m%d_%H%M%S)"
  REQUEST_FILE_HOST="${REQUEST_FILE_HOST%/}/restore_requests_${timestamp}.txt"
fi

if [[ -n "${ARRIVAL_FILE_HOST}" ]]; then
  if [[ -d "${ARRIVAL_FILE_HOST}" ]]; then
    timestamp="$(date +%Y%m%d_%H%M%S)"
    ARRIVAL_FILE_HOST="${ARRIVAL_FILE_HOST%/}/restore_arrivals_${timestamp}.tsv"
  elif [[ "${ARRIVAL_FILE_HOST}" == */ ]]; then
    mkdir -p "${ARRIVAL_FILE_HOST%/}"
    timestamp="$(date +%Y%m%d_%H%M%S)"
    ARRIVAL_FILE_HOST="${ARRIVAL_FILE_HOST%/}/restore_arrivals_${timestamp}.tsv"
  fi
elif [[ "${POISSON_RATE}" != "0" && "${POISSON_RATE}" != "0.0" ]]; then
  ARRIVAL_FILE_HOST="${REQUEST_FILE_HOST%.*}_arrivals.tsv"
fi

mkdir -p "$(dirname "${REQUEST_FILE_HOST}")"
if [[ -n "${ARRIVAL_FILE_HOST}" ]]; then
  mkdir -p "$(dirname "${ARRIVAL_FILE_HOST}")"
fi

input_to_container_path() {
  local host_path="$1"
  local host_abs
  host_abs="$(readlink -f "${host_path}")"
  if [[ "${host_abs}" == "${DATA_ROOT_ABS}" ]]; then
    echo "/input"
    return 0
  fi
  if [[ "${host_abs}" == "${DATA_ROOT_ABS}/"* ]]; then
    local rel="${host_abs#${DATA_ROOT_ABS}/}"
    echo "/input/${rel}"
    return 0
  fi
  echo "ERROR: input path is outside DATA_ROOT: ${host_abs} (DATA_ROOT=${DATA_ROOT_ABS})" >&2
  return 1
}

INPUT_DIR_CONTAINER="$(input_to_container_path "${INPUT_DIR_ABS}")"

tmp_list="$(mktemp)"
tmp_sample="$(mktemp)"
trap 'rm -f "${tmp_list}" "${tmp_sample}"' EXIT

file_count=0
> "${REQUEST_FILE_HOST}"

if [[ -n "${FILEINFO_JSON}" ]]; then
  python3 - <<'PY' "${FILEINFO_JSON}" "${INPUT_DIR_ABS}" "${INCLUDE_PATTERN}" "${EXCLUDE_PATTERN}" "${REQUEST_COUNT}" "${REQUEST_MULTIPLIER}" "${SAMPLE_WITH_REPLACEMENT}" "${RANDOM_SEED}" "${VERIFY_EXISTS}" "${tmp_sample}"
import fnmatch
import json
import os
import random
import sys

(
    fileinfo_json,
    input_dir_abs,
    include_pattern,
    exclude_pattern,
    request_count_s,
    request_multiplier_s,
    with_replacement_s,
    seed_s,
    verify_exists_s,
    out_path,
) = sys.argv[1:11]

request_count = int(request_count_s)
request_multiplier = int(request_multiplier_s)
with_replacement = with_replacement_s == "1"
verify_exists = verify_exists_s == "1"

seed = None
if seed_s != "":
    seed = int(seed_s)
rng = random.Random(seed)

with open(fileinfo_json, "r", encoding="utf-8") as f:
    data = json.load(f)

files = data.get("files", [])
if request_count <= 0:
    request_count = request_multiplier * len(files)

if request_count <= 0:
    raise SystemExit(
        "ERROR: effective request_count <= 0; check --request-count / --request-multiplier and fileinfo files count"
    )

candidates = []
weights = []
for item in files:
    rel = str(item.get("path", "")).strip().lstrip("/")
    if not rel:
        continue
    base = os.path.basename(rel)
    if not fnmatch.fnmatch(base, include_pattern):
        continue
    if exclude_pattern and fnmatch.fnmatch(base, exclude_pattern):
        continue
    if verify_exists:
        host_path = os.path.join(input_dir_abs, rel)
        if not os.path.isfile(host_path):
            continue

    heat_val = item.get("heat", 0.0)
    try:
        heat = float(heat_val)
    except (TypeError, ValueError):
        heat = 0.0
    if heat < 0:
        heat = 0.0

    candidates.append(rel)
    weights.append(heat)

if not candidates:
    raise SystemExit("ERROR: no candidate files after filtering fileInfo and input dir checks")

if sum(weights) <= 0:
    weights = [1.0] * len(candidates)

if not with_replacement and request_count > len(candidates):
    raise SystemExit(
        f"ERROR: request_count={request_count} exceeds unique candidates={len(candidates)} in without-replacement mode"
    )

selected = []
if with_replacement:
    selected = rng.choices(candidates, weights=weights, k=request_count)
else:
    remain_items = list(candidates)
    remain_weights = list(weights)
    for _ in range(request_count):
        pos = rng.choices(range(len(remain_items)), weights=remain_weights, k=1)[0]
        selected.append(remain_items.pop(pos))
        remain_weights.pop(pos)

with open(out_path, "w", encoding="utf-8") as f:
    for rel in selected:
        f.write(f"{rel}\n")
PY

  while IFS= read -r rel; do
    [[ -z "${rel}" ]] && continue
    in_path="${INPUT_DIR_CONTAINER}/${rel}"
    out_path="${OUTPUT_DIR}/${rel}"
    printf "%s\t%s\n" "${in_path}" "${out_path}" >> "${REQUEST_FILE_HOST}"
    file_count=$((file_count + 1))
  done < "${tmp_sample}"
else
  find "${INPUT_DIR_ABS}" -type f | sort > "${tmp_list}"

  while IFS= read -r host_file; do
    base_name="$(basename "${host_file}")"
    if [[ ! "${base_name}" == ${INCLUDE_PATTERN} ]]; then
      continue
    fi
    if [[ -n "${EXCLUDE_PATTERN}" && "${base_name}" == ${EXCLUDE_PATTERN} ]]; then
      continue
    fi

    rel="${host_file#${INPUT_DIR_ABS}/}"
    if [[ "${host_file}" == "${INPUT_DIR_ABS}" ]]; then
      rel="$(basename "${host_file}")"
    fi

    in_path="${INPUT_DIR_CONTAINER}/${rel}"
    out_path="${OUTPUT_DIR}/${rel}"
    printf "%s\t%s\n" "${in_path}" "${out_path}" >> "${REQUEST_FILE_HOST}"
    file_count=$((file_count + 1))

    if [[ "${MAX_FILES}" -gt 0 && "${file_count}" -ge "${MAX_FILES}" ]]; then
      break
    fi
  done < "${tmp_list}"
fi

if [[ "${file_count}" -eq 0 ]]; then
  echo "ERROR: no files matched. input=${INPUT_DIR_ABS} include=${INCLUDE_PATTERN} exclude=${EXCLUDE_PATTERN}" >&2
  exit 1
fi

if [[ -n "${ARRIVAL_FILE_HOST}" ]]; then
  python3 - <<'PY' "${REQUEST_FILE_HOST}" "${ARRIVAL_FILE_HOST}" "${POISSON_RATE}" "${RANDOM_SEED}"
import random
import sys

request_path, arrival_path, poisson_rate_s, seed_s = sys.argv[1:5]
poisson_rate = float(poisson_rate_s)

seed = None
if seed_s != "":
    seed = int(seed_s)
rng = random.Random(seed)

requests = []
with open(request_path, "r", encoding="utf-8") as f:
    for line in f:
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if "\t" in raw:
            in_path, out_path = raw.split("\t", 1)
        else:
            parts = raw.split()
            if len(parts) != 2:
                continue
            in_path, out_path = parts
        requests.append((in_path.strip(), out_path.strip()))

with open(arrival_path, "w", encoding="utf-8") as f:
    f.write("# arrival_sec\tinput_path\toutput_path\n")
    t = 0.0
    for in_path, out_path in requests:
        if poisson_rate > 0:
            t += rng.expovariate(poisson_rate)
        f.write(f"{t:.6f}\t{in_path}\t{out_path}\n")
PY
fi

echo "[restore-batch] request file generated: ${REQUEST_FILE_HOST}"
echo "[restore-batch] entries: ${file_count}"
echo "[restore-batch] mode: $([[ -n "${FILEINFO_JSON}" ]] && echo "fileinfo-weighted" || echo "directory-scan")"
echo "[restore-batch] input dir (host): ${INPUT_DIR_ABS}"
echo "[restore-batch] input dir (container): ${INPUT_DIR_CONTAINER}"
echo "[restore-batch] output dir (container): ${OUTPUT_DIR}"
if [[ -n "${FILEINFO_JSON}" ]]; then
  echo "[restore-batch] fileinfo json: ${FILEINFO_JSON}"
  if [[ "${REQUEST_COUNT}" -gt 0 ]]; then
    echo "[restore-batch] request-count: ${REQUEST_COUNT}"
  else
    echo "[restore-batch] request-multiplier: ${REQUEST_MULTIPLIER}"
    echo "[restore-batch] effective request-count: ${file_count}"
  fi
  echo "[restore-batch] replacement: $([[ "${SAMPLE_WITH_REPLACEMENT}" -eq 1 ]] && echo "yes" || echo "no")"
fi
if [[ -n "${ARRIVAL_FILE_HOST}" ]]; then
  echo "[restore-batch] arrival trace: ${ARRIVAL_FILE_HOST}"
  echo "[restore-batch] poisson-rate(req/s): ${POISSON_RATE}"
fi

if [[ "${RUN_RESTORE}" -ne 1 ]]; then
  echo "[restore-batch] dry run complete. add --run to execute restore-batch."
  exit 0
fi

if [[ -n "${ARRIVAL_FILE_HOST}" && "${POISSON_RATE}" != "0" && "${POISSON_RATE}" != "0.0" ]]; then
  echo "[restore-batch] note: --run submits all requests in one batch; poisson schedule is only written to trace file."
fi

STORE_MANAGER_DIR="$(readlink -f "${STORE_ROOT}/manager")"
mkdir -p "${STORE_MANAGER_DIR}"
REQ_HOST_ABS="$(readlink -f "${REQUEST_FILE_HOST}")"

if [[ "${REQ_HOST_ABS}" == "${STORE_MANAGER_DIR}/"* ]]; then
  if [[ -z "${REQUEST_FILE_CONTAINER}" ]]; then
    rel_req="${REQ_HOST_ABS#${STORE_MANAGER_DIR}/}"
    REQUEST_FILE_CONTAINER="/data/${rel_req}"
  fi
else
  req_name="restore_requests_$(date +%Y%m%d_%H%M%S).txt"
  cp "${REQ_HOST_ABS}" "${STORE_MANAGER_DIR}/${req_name}"
  REQUEST_FILE_CONTAINER="/data/${req_name}"
fi

echo "[restore-batch] running manager restore-batch ..."
echo "[restore-batch] compose: ${COMPOSE_FILE}"
echo "[restore-batch] env: ${ENV_FILE}"
echo "[restore-batch] request (container): ${REQUEST_FILE_CONTAINER}"
if [[ -n "${PLACEMENT_JSON}" ]]; then
  echo "[restore-batch] placement-json: ${PLACEMENT_JSON}"
fi
if [[ -n "${PLACEMENT_DIR}" ]]; then
  echo "[restore-batch] placement-dir: ${PLACEMENT_DIR}"
fi
if [[ -n "${PLACEMENT_STRATEGY}" ]]; then
  echo "[restore-batch] placement-strategy: ${PLACEMENT_STRATEGY}"
fi

MANAGER_CMD=(manager -config "${MANAGER_CONFIG}")
if [[ -n "${PLACEMENT_JSON}" ]]; then
  MANAGER_CMD+=(-placement-json "${PLACEMENT_JSON}")
fi
if [[ -n "${PLACEMENT_DIR}" ]]; then
  MANAGER_CMD+=(-placement-dir "${PLACEMENT_DIR}")
fi
if [[ -n "${PLACEMENT_STRATEGY}" ]]; then
  MANAGER_CMD+=(-placement-strategy "${PLACEMENT_STRATEGY}")
fi
MANAGER_CMD+=(restore-batch "${REQUEST_FILE_CONTAINER}")

docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" run --rm manager "${MANAGER_CMD[@]}"
