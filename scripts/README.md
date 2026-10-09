# Scripts 说明

本目录按用途分组：

- `restore/`：恢复相关脚本
- `network/`：网络仿真辅助脚本
- `config/`：集群配置批量生成脚本
- `fileinfo_builder/`：生成 `mean_go_v1` 输入元数据
- `case1_cluster/`：聚类 + Case1 + 块到边缘节点映射生成
- `data/input/`：样例输入数据
- `data/output/`：样例输出结果

## 目录结构

```text
disDedup/scripts/
  README.md
  restore/
    batch_restore_from_dir.sh
  network/
    netem.sh
  config/
    sync_cluster_configs.sh
  fileinfo_builder/
    README.md
    build_fileInfo_from_dataset.go
    go.mod
    go.sum
  case1_cluster/
    README.md
    run_case1_cluster.py
  data/
    input/
      fileInfo-src.json
    output/
      case1_cluster_python_compact_with_mapping_balanced.json
```

## 批量恢复脚本

在 `disDedup` 目录执行：

```bash
cd /path/to/edgededup/disDedup
```

仅生成请求文件（不执行恢复）：

```bash
./scripts/restore/batch_restore_from_dir.sh \
  --input-dir /mnt/test/dedup_datasets/github_repo \
  --output-dir /mnt/test/dedup/manager/restore
```

按 `fileInfo-src.json` 的 `files[].heat` 做加权随机抽样，并生成泊松到达轨迹：

```bash
./scripts/restore/batch_restore_from_dir.sh \
  --input-dir /mnt/test/dedup_datasets/github_repo \
  --fileinfo-json ./scripts/data/input/fileInfo-src.json \
  --request-count 200 \
  --seed 42 \
  --poisson-rate 5 \
  --request-file-host /mnt/test/dedup_chunk/manager/restore_requests_hot.txt \
  --arrival-file-host /mnt/test/dedup_chunk/manager/restore_arrivals_hot.tsv
```

说明：
- `--request-count`：抽样请求数量（必填，`--fileinfo-json` 模式下）。
- `--sample-without-replacement`：可切换为不放回抽样（默认放回）。
- `--poisson-rate`：请求到达率（req/s），到达时间写入 `arrival` 文件。

按泊松到达文件回放恢复请求，并自动汇总总性能/平均性能：

```bash
./scripts/restore/replay_restore_poisson.py \
  --arrival-file-host /mnt/test/dedup/manager/restore/restore_requests_10p_arrivals.tsv \
  --env-file ./compose.paths.env \
  --compose-file ./docker-compose.yml \
  --placement-json /data/placement/case1_cluster_python_8KB_fileInfo-zipf-s0.7.json
```

说明：回放会使用常驻 `manager` 容器并通过 `docker exec` 发起请求，请先确保 `manager` 服务已启动。
可选覆盖参数：`--placement-json` / `--placement-dir` / `--placement-strategy`（会透传给 `manager` 命令）。

仅走云端全量恢复回放可加：

```bash
./scripts/restore/replay_restore_poisson.py \
  --arrival-file-host /mnt/test/dedup/manager/restore/restore_requests_10p_arrivals.tsv \
  --env-file ./compose.paths.env \
  --compose-file ./docker-compose.yml \
  --cloud-only
```

运行后会在 `${STORE_ROOT}/manager/restore_poisson_replay_<timestamp>/` 输出：
- `summary.json`：总性能与平均性能（吞吐、总字节、平均/分位时延等）
- `replay_detail.json`：每条请求的执行记录（退出码、耗时、日志尾部）
- `stats_snapshots/restore_stats_*.json`：每次恢复后的 manager 统计快照

按一批固定恢复请求导出实验 CSV：

```bash
./scripts/restore/export_restore_experiment_csv.py \
  --request-file-host /mnt/test/dedup/manager/restore/restore_requests_hot.txt \
  --env-file ./compose.paths.env \
  --compose-file ./docker-compose.yml
```

默认会对同一批文件依次运行：
- `edge-all`
- `cloud-full`
- `hybrid`，`edge_ratio=0.95, 0.90, ..., 0.50`

输出目录为 `${STORE_ROOT}/manager/restore_experiment_csv_<timestamp>/`，主要文件：
- `restore_experiment_rows.csv`：扁平结果表，一行表示“一个文件在一个恢复场景下”的结果
- `restore_experiment_rows.json`：同内容 JSON 版
- `stats_snapshots/<scenario>/restore_stats_*.json`：每次恢复对应的原始 manager 统计快照

生成后立即执行批量恢复：

```bash
./scripts/restore/batch_restore_from_dir.sh \
  --input-dir /mnt/test/dedup_datasets/github_repo \
  --output-dir /output/github_repo_restored \
  --placement-json /data/placement/case1_cluster_python_8KB_fileInfo-zipf-s0.7.json \
  --run
```

## 批量配置脚本

一次生成/更新 manager、cloud、edge1..N 的 YAML：

```bash
./scripts/config/sync_cluster_configs.sh \
  --env-file ./compose.paths.env \
  --edge-count 10 \
  --storage-granularity block \
  --chunk-size 8192 \
  --placement-json /data/placement/case1_cluster_python_8KB_fileInfo-zipf-s0.7.json \
  --placement-strategy round_robin \
  --restore-cloud-full-threshold 1
```

生成后重启容器使配置生效：

```bash
docker compose --env-file compose.paths.env up -d --force-recreate
```

## 一键实验入口

如果你希望从“原始数据集目录”开始，自动完成：

1. 生成 `fileInfo JSON`
2. 按 Zipf 或外部热度设置 `heat`
3. 生成 placement
4. 运行 dedup / restore 实验
5. 汇总矩阵结果

可以直接使用：

```bash
python3 ./scripts/experiments/run_dataset_experiment.py \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --method topfirst_v2 \
  --zipf-s 0.7 \
  --chunk-bytes 8192 \
  --cap-values 20,25,30
```

一次跑多个方法：

```bash
python3 ./scripts/experiments/run_dataset_experiment.py \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --method topfirst_v2 \
  --method directprotect_v2 \
  --zipf-s 0.7 \
  --chunk-bytes 8192 \
  --cap-values 5,10,15,20,25,30
```

仅生成 `fileInfo JSON`，不启动实验：

```bash
python3 ./scripts/experiments/run_dataset_experiment.py \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --method topfirst_v2 \
  --zipf-s 0.7 \
  --build-only
```

说明：

- 默认会复用已经存在的 `fileInfo JSON`
- 加 `--force-rebuild-fileinfo` 可强制重建
- `--method` 可以填脚本别名，也可以直接填算法脚本路径
- 恢复请求文件仍然由内部 `run_topfirst_shuffle_suite.py` 按 `fileInfo.files[].heat` 自动生成
