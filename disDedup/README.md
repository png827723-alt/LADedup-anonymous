# Distributed Dedup Prototype (Manager + Edge Nodes + Cloud)

This is a distributed refactor of the original single-node dedup prototype.

## Roles

1. **Management Node & Client (manager)**
   - Holds **global metadata**: `chunk_hash -> edge_node_id`
   - Generates and stores per-file **recipe** (`.recipe`) files
   - Performs chunking + SHA-1, chooses an edge node by a placement strategy, and sends data via **gRPC**
   - Always replicates every new chunk to a **Cloud Server** as a reliable backup
   - Restores files by batching requests per edge node (e.g., 128 hashes/request)

2. **Edge Storage Node (edge)**
   - Passive chunk warehouse
   - Maintains **local index**: `chunk_hash -> local_location` (block path or container ID)
   - Stores data using the same `block` or `container` engines (configurable)
   - Responds to manager's batched GET requests

3. **Cloud Server (cloud)**
   - Same implementation as an edge node, but used as the *reliable backup*.

## gRPC

This prototype uses **grpc-go** transport, with a JSON codec (no `.proto` generation needed).

## Quick start (local)

Terminal A (cloud):

```bash
go run ./cmd/storage-node -config ./examples/cloud.yaml
```

Terminal B (edge1):

```bash
go run ./cmd/storage-node -config ./examples/edge1.yaml
```

Terminal C (edge2):

```bash
go run ./cmd/storage-node -config ./examples/edge2.yaml
```

Terminal D (manager):

```bash
go run ./cmd/manager -config ./examples/manager.yaml dedup /path/to/input
go run ./cmd/manager -config ./examples/manager.yaml restore /path/to/input/file /path/to/output/file
```

## Notes

* Placement strategy: `round_robin` or `random`
* Restore uses two-stage fetch:
  - Fetch from the owning edge node (grouped + batched)
  - Fallback to cloud for missing/unavailable chunks
* Manager config supports `restore_discard_output: true`:
  - restore still reconstructs file content successfully
  - but skips writing the final result into the output directory
* Manager persists statistics JSON after each completed task:
  - `dedup`: `<stats_path>/dedup_stats.json`
  - `restore`: `<stats_path>/restore_stats.json`
  - If `stats_path` is empty, default is `<recipe_path>/stats`
  - Each stats file includes `phase_breakdown` (duration + percentage by stage)
* Global metadata is persisted as a flat JSON map:
  - `chunk_hash -> node_number` (edge nodes are `1..N` by `edge_nodes` order, cloud-only fallback is `0`)
* Storage nodes (edge/cloud) persist stage timing stats after every `Flush`:
  - file: `<node_stats_path>/node_perf_stats.json`
  - if `node_stats_path` is empty, default is `<dirname(storage_path)>/stats`
  - includes dedup (`PutChunk`) / restore (`BatchGet`) / flush phase duration and percentage
* `examples/*.yaml` are templates only.
  - In Docker Compose runtime, effective configs come from `compose.paths.env` variables:
    `MANAGER_CFG_PATH`, `CLOUD_CFG_PATH`, `EDGE1_CFG_PATH` ... `EDGE10_CFG_PATH`


## 实验流程

下面这套流程按 5 步走：

1. 把原始数据集转换成 `mean_go_v1` JSON
2. 选择一个文件选择算法，得到块映射文件 `placement.json`
3. 新建一个新的实验目录，手动复制 cloud 侧 `_fullfiles`，不走 `cloud-sync`
4. 按放置策略做去重并把块分发到 edge/cloud
5. 按热度生成恢复请求文件，再根据这个请求文件恢复并统计数据

以下示例假设：

- 原始数据集根目录：`/mnt/test/dedup_datasets/github_repo`
- 旧实验根目录：`/mnt/test/dedup_case1_orig`
- 新实验根目录：`/mnt/test/dedup_case1_run1`
- edge 数量：`10`
- 分块大小：`8192`

### 0. 准备新的实验目录

先复制一份 env 文件，给这次实验单独指定 `STORE_ROOT`：

```bash
cd /path/to/edgededup/disDedup

cp compose.paths.env compose.case1.env
```

把 `compose.case1.env` 里的 `STORE_ROOT` 改成新的实验目录，例如：

```bash
STORE_ROOT=/mnt/test/dedup_case1_run1
DATA_ROOT=/mnt/test/dedup_datasets
```

然后初始化目录：

```bash
NEW_STORE=/mnt/test/dedup_case1_run1

mkdir -p "${NEW_STORE}/manager/placement"
mkdir -p "${NEW_STORE}/manager/restore"
mkdir -p "${NEW_STORE}/output"
mkdir -p "${NEW_STORE}/cloud/storage"

for i in $(seq 1 10); do
  mkdir -p "${NEW_STORE}/edge${i}"
done
```

### 1. 将数据集转换成 JSON

这一步统一使用 `scripts/fileinfo_builder/build_fileInfo_from_dataset.go`，输出的是后续各类文件选择算法都能直接使用的 `mean_go_v1` JSON。

```bash
FILEINFO_JSON="${NEW_STORE}/manager/placement/fileInfo-zipf-s0.7-8KB.json"

cd /path/to/edgededup/disDedup/scripts/fileinfo_builder

go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json "${FILEINFO_JSON}" \
  --chunk-bytes 8192 \
  --zipf-s 0.7 \
  --progress-every 500 \
  --pretty
```

如果你已经有外部热度文件，可以改用：

```bash
go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json "${FILEINFO_JSON}" \
  --chunk-bytes 8192 \
  --popularity-csv /path/to/file_heat.csv \
  --progress-every 500 \
  --pretty
```

### 2. 选择文件选择算法，生成块映射文件

所有算法的目标都是生成一个 manager 可用的块放置文件。最终给 manager 用的 JSON 至少需要包含：

- 直接的 `{"<chunk_hash>":"<edge_node_id>", ...}`
- 或顶层带 `hash2edge_node_id`

#### 2.1 Case1 系列脚本

这组脚本都直接吃第 1 步生成的 `mean_go_v1` JSON，并直接输出带 `hash2edge_node_id` 的结果文件：

- `scripts/case1_cluster/run_case1_cluster.py`
- `scripts/case1_cluster/run_case1_cluster_heat_only_spread.py`
- `scripts/case1_cluster/run_case1_cluster_restore_weighted.py`
- `scripts/case1_cluster/run_case1_cluster_theoretical_weighted.py`
- `scripts/case1_cluster/run_case1_cluster_theoretical_weighted_topfirst.py`

最基础的 Case1 例子：

```bash
cd /path/to/edgededup/disDedup/scripts/case1_cluster

python3 run_case1_cluster.py \
  --input-json "${FILEINFO_JSON}" \
  --output-json "${NEW_STORE}/manager/placement/case1_cluster_8KB.json" \
  --k 1000 \
  --alpha 5 \
  --capacity-ratio 0.2 \
  --server-num 10 \
  --workers 8 \
  --no-progress
```

如果要换算法，只需要把脚本名替换掉，`--input-json` 和 `--output-json` 的用法不变。

#### 2.2 HotDedup 脚本

如果你要跑 `scripts/hotdedup/hot.go`，它不直接吃 `mean_go_v1`，而是先要把 `fileInfo` 转成 HotDedup 输入：

```bash
cd /path/to/edgededup/disDedup

python3 ./scripts/hotdedup/build_hot_input.py \
  --fileinfo-json "${FILEINFO_JSON}" \
  --request-file "${NEW_STORE}/manager/restore/restore_requests_seed.txt" \
  --output-json "${NEW_STORE}/manager/placement/hot_input.json" \
  --edge-count 10 \
  --capacity-ratio 0.2 \
  --dataset-prefix /input/github_repo

go run ./scripts/hotdedup/hot.go \
  -in "${NEW_STORE}/manager/placement/hot_input.json" \
  -out "${NEW_STORE}/manager/placement/hot_output.json" \
  -prizeScale 1000 \
  -roots 32

python3 ./scripts/hotdedup/hot_output_to_placement.py \
  --hot-output "${NEW_STORE}/manager/placement/hot_output.json" \
  --placement-json "${NEW_STORE}/manager/placement/hot_placement.json"
```

如果你走 Case1 系列，后面 dedup 时直接用对应的 `case1_cluster_8KB.json` 即可；如果你走 HotDedup，则用最后转出来的 `hot_placement.json`。

### 3. 手动复制 cloud 全文件，不走 cloud-sync

这一步不要执行 `manager cloud-sync`。直接把旧实验里的 full file 数据复制到新实验的 cloud 目录。

当前 cloud 侧全文件对象默认在：

```bash
/mnt/test/dedup_cdedup_cluster/cloud/storage/_fullfiles
```

复制到新实验目录：

```bash
cp -r \
  /mnt/test/dedup_case1_orig/cloud/storage/_fullfiles \
  /mnt/test/dedup_case1_run1/cloud/storage/
```

复制完成后，新实验目录应该至少有：

```bash
/mnt/test/dedup_case1_run1/cloud/storage/_fullfiles
```

### 4. 根据放置策略进行去重并放置块

先生成 manager/cloud/edge 的配置文件。下面以 `case1_cluster_8KB.json` 为例：

```bash
cd /path/to/edgededup/disDedup

./scripts/config/sync_cluster_configs.sh \
  --env-file ./compose.case1.env \
  --edge-count 10 \
  --chunk-size 8192 \
  --storage-granularity block \
  --chunking-method fastcdc \
  --placement-json /data/placement/case1_cluster_8KB.json \
  --placement-strategy round_robin \
  --restore-cloud-full-threshold 1
```

然后构建镜像并启动服务：

```bash
mkdir -p bin
CGO_ENABLED=0 GOOS=linux go build -o bin/manager ./cmd/manager
CGO_ENABLED=0 GOOS=linux go build -o bin/storage-node ./cmd/storage-node
docker build -t disdedup:latest .

docker compose --env-file ./compose.case1.env up -d cloud \
  edge1 edge2 edge3 edge4 edge5 edge6 edge7 edge8 edge9 edge10 manager
```

确认 `placement.json` 已经放在新实验目录下的 manager 可见路径：

```bash
ls -lah "${NEW_STORE}/manager/placement"
```

执行去重：

```bash
docker compose --env-file ./compose.case1.env run --rm manager \
  manager -config /cfg/manager.yaml \
  -placement-json /data/placement/case1_cluster_8KB.json \
  dedup /input/github_repo
```

如果你换了别的算法，只需要把 `-placement-json` 指向对应输出文件，例如：

- `/data/placement/hot_placement.json`
- `/data/placement/run_case1_cluster_restore_weighted.json`

去重统计默认会写到：

```bash
${NEW_STORE}/manager/stats/dedup_stats.json
```

### 5. 根据热度生成恢复请求文件，并恢复统计

先根据 `fileInfo` 里的 `files[].heat` 生成恢复请求文件：

```bash
REQUEST_FILE_HOST="${NEW_STORE}/manager/restore/restore_requests_8KB_fileInfo-zipf-s0.7_mult2.txt"

cd /path/to/edgededup/disDedup

./scripts/restore/batch_restore_from_dir.sh \
  --input-dir /mnt/test/dedup_datasets/github_repo \
  --fileinfo-json "${FILEINFO_JSON}" \
  --request-multiplier 2 \
  --request-file-host "${REQUEST_FILE_HOST}"
```

如果你需要泊松到达轨迹，可以额外生成：

```bash
./scripts/restore/batch_restore_from_dir.sh \
  --input-dir /mnt/test/dedup_datasets/github_repo \
  --fileinfo-json "${FILEINFO_JSON}" \
  --request-multiplier 2 \
  --request-file-host "${REQUEST_FILE_HOST}" \
  --poisson-rate 5 \
  --arrival-file-host "${NEW_STORE}/manager/restore/restore_arrivals_8KB_fileInfo-zipf-s0.7_mult2.tsv"
```

然后根据这个请求文件做恢复：

```bash
docker compose --env-file ./compose.case1.env run --rm manager \
  manager -config /cfg/manager.yaml \
  restore-batch /data/restore/restore_requests_8KB_fileInfo-zipf-s0.7_mult2.txt
```

恢复统计默认会写到：

```bash
${NEW_STORE}/manager/stats/restore_stats.json
```

如果要按到达轨迹回放并统计整批结果，可以用：

```bash
./scripts/restore/replay_restore_poisson.py \
  --arrival-file-host "${NEW_STORE}/manager/restore/restore_arrivals_8KB_fileInfo-zipf-s0.7_mult2.tsv" \
  --env-file ./compose.case1.env \
  --compose-file ./docker-compose.yml
```

回放统计会额外输出到：

```bash
${NEW_STORE}/manager/restore_poisson_replay_<timestamp>/
```

## 这套流程里最关键的几个文件

- 数据集元数据：`${NEW_STORE}/manager/placement/fileInfo-*.json`
- 块放置文件：`${NEW_STORE}/manager/placement/*.json`
- cloud 全文件目录：`${NEW_STORE}/cloud/storage/_fullfiles`
- 去重统计：`${NEW_STORE}/manager/stats/dedup_stats.json`
- 恢复请求文件：`${NEW_STORE}/manager/restore/*.txt`
- 恢复统计：`${NEW_STORE}/manager/stats/restore_stats.json`
