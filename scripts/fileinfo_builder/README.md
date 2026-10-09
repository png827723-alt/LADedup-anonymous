# FileInfo Builder

这个文档说明如何使用 `disDedup/scripts/fileinfo_builder/build_fileInfo_from_dataset.go` 生成聚类输入 JSON。

## 1. 脚本作用

脚本会：

1. 扫描 `--dataset-root` 下所有文件  
2. 用 **FastCDC** 分块  
3. 对每个块计算 SHA-1，生成全局唯一块字典  
4. 为每个文件生成 `chunk_ids`、`chunk_sizes`、`heat`、`heat_dsize`  
5. 输出聚类可用 JSON（默认文件名 `datasets/fileInfo-src.json`）

脚本不会：

1. 不会把块数据写入磁盘  
2. 不会写入 dedup 存储引擎  
3. 不会生成配方 `.recipe`

## 2. FastCDC 分块参数

`--chunk-bytes` 是 FastCDC 的目标块大小（target）。

脚本内部配置为：

1. `min = target / 4`  
2. `target = chunk-bytes`  
3. `max = target * 4`

例如默认 `--chunk-bytes=4096`：

1. 最小块 `1024`  
2. 目标块 `4096`  
3. 最大块 `16384`

## 3. 热度（Heat）规则

默认使用 Zipf 分布：

`heat = 1 / rank^s`

其中：

1. `s` 由 `--zipf-s` 指定，默认 `0.7`  
2. 可选 `--zipf-random` 将 rank 随机打散  
3. `--zipf-seed` 控制随机种子（同 seed 可复现）

如果提供 `--popularity-csv`（列 `file,heat`），CSV 中命中的文件会覆盖默认 Zipf 热度。

## 4. 运行方法

进入目录：

```bash
cd /path/to/edgededup/disDedup/scripts/fileinfo_builder
```

基础示例（默认 Zipf，非随机）：

```bash
go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json /mnt/test/dedup/fileInfo-src.json \
  --chunk-bytes 4096 \
  --zipf-s 0.7 \
  --progress-every 500 \
  --pretty
```

随机 Zipf 示例：

```bash
go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json /mnt/test/dedup/fileInfo-src.json \
  --zipf-s 0.7 \
  --zipf-random \
  --zipf-seed 42 \
  --pretty
```

CSV 覆盖热度示例：

```bash
go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json /mnt/test/dedup/fileInfo-src.json \
  --popularity-csv /mnt/test/dedup/file_heat.csv
```

## 5. 参数说明

1. `--dataset-root`：数据集根目录（必填）
2. `--output-json`：输出 JSON 路径（建议放到 `../data/input/fileInfo-src.json`）
3. `--chunk-bytes`：FastCDC 目标块大小
4. `--zipf-s`：Zipf 指数（默认 `0.7`）
5. `--zipf-random`：是否随机打散 rank
6. `--zipf-seed`：随机种子（`0` 表示当前时间）
7. `--popularity-csv`：可选 CSV 覆盖热度
8. `--progress-every`：每处理 N 个文件打印进度
9. `--pretty`：格式化输出 JSON

## 6. 输出 JSON 结构

顶层字段：

1. `format_version`：当前为 `mean_go_v1`
2. `chunk_bytes`：FastCDC target
3. `total_size`：数据集总字节数
4. `uni_fingerprint`：唯一块 SHA-1 列表
5. `uni_size`：唯一块大小列表
6. `files`：文件级信息数组

`files[i]` 字段：

1. `path`：相对 `dataset-root` 路径
2. `chunk_ids`：块 ID 序列（1-based）
3. `chunk_sizes`：块大小序列
4. `heat`：文件热度
5. `heat_dsize`：`heat / 文件总字节数`

```bash
go run . \
  --dataset-root /mnt/test/dedup_datasets/github_repo \
  --output-json /path/to/edgededup/disDedup/scripts/data/input/fileInfo-src.json \
  --zipf-s 0.7 \
  --zipf-random \
  --chunk-bytes 8192 \
  --zipf-seed 42 \
  --pretty
```
