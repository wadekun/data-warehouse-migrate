# MaxCompute 分区表查询支持 — 设计文档

## 1. 问题背景

### 1.1 现状

当 MaxCompute 作为数据源时，`MaxComputeClient._build_select_sql` 方法会自动检测分区表并处理分区条件，当前逻辑如下：

1. **有 `pt` 分区字段**：自动通过 `SELECT MAX(pt)` 获取最新分区值，添加 `WHERE pt = '最新值'`。
2. **有其他分区字段（非 `pt`）**：对每个分区字段独立取 `MAX()` 值，组合成 `WHERE col1='v1' AND col2='v2'`。
3. **分区无数据**：跳过分区条件，自动添加 `LIMIT 100000` 防止全表扫描。
4. **非分区表**：直接 `SELECT * FROM table_name`，不添加任何分区条件。

### 1.2 存在的问题

| 问题 | 说明 |
|------|------|
| 只能查最新分区 | 硬编码取 `MAX(pt)`，无法查询历史分区或多个分区范围 |
| 多级分区取值可能不准确 | `_get_latest_partitions` 对每个分区字段独立取 `MAX`，在多级分区场景（如 `pt='20240101'/region='us'`）下可能拼出不存在的分区组合 |
| 无法查全量数据 | 分区表始终只查一个分区，无法指定查询所有分区 |
| 分区字段名硬编码 | 特殊处理 `pt` 字段名，但实际业务中分区字段可能叫 `ds`、`dt`、`date_key` 等 |
| 无用户控制能力 | CLI、配置文件、`get_table_data` 方法均未暴露分区参数，用户无法干预分区选择 |

### 1.3 涉及的代码

| 文件 | 关键方法/位置 |
|------|--------------|
| `data_warehouse_migrate/maxcompute_client.py` | `_build_select_sql`、`get_table_data`、`_get_latest_partition`、`_get_latest_partitions` |
| `data_warehouse_migrate/migrator.py` | `_migrate_table_data`（调用 `get_table_data` 时未传递分区信息） |
| `data_warehouse_migrate/cli.py` | CLI 参数定义（缺少分区相关选项） |
| `data_warehouse_migrate/config_loader.py` | 配置文件规范化（缺少分区字段解析） |

---

## 2. 设计方案

### 2.1 总体思路

引入 `partition_filter` 参数，贯穿从配置文件/CLI → 迁移器 → MaxCompute 客户端的完整调用链：

- 用户指定了 `partition_filter` → 直接使用用户提供的分区条件构建 WHERE 子句
- 用户未指定 → 保持现有自动取最新分区的行为作为 fallback

### 2.2 参数格式设计

`partition_filter` 为字符串类型，直接作为 SQL 的 WHERE 条件片段拼接，支持以下用法：

```
# 单分区精确匹配
partition_filter: "pt='20240501'"

# 分区范围查询
partition_filter: "pt>='20240401' AND pt<='20240501'"

# 多级分区
partition_filter: "pt='20240501' AND region='us'"

# 查询所有分区（使用特殊值）
partition_filter: "*"
```

当值为 `*` 时，表示不添加任何分区条件，查询全部分区数据。

### 2.3 各模块改动

#### 2.3.1 `maxcompute_client.py`

**`get_table_data` 增加 `partition_filter` 参数：**

```python
def get_table_data(self, table_name: str, limit: Optional[int] = None,
                   batch_size: int = 10000,
                   partition_filter: Optional[str] = None) -> Iterator[pd.DataFrame]:
    table = self.odps.get_table(table_name)
    sql = self._build_select_sql(table, table_name, limit,
                                 partition_filter=partition_filter)
    # ... 其余逻辑不变
```

**`_build_select_sql` 增加 `partition_filter` 参数：**

```python
def _build_select_sql(self, table, table_name: str,
                      limit: Optional[int] = None,
                      partition_filter: Optional[str] = None) -> str:
    sql = f"SELECT * FROM {table_name}"

    if partition_filter:
        if partition_filter.strip() == '*':
            # 查询所有分区，不添加 WHERE 条件
            logger.info(f"用户指定查询所有分区数据")
        else:
            sql += f" WHERE {partition_filter}"
            logger.info(f"使用用户指定的分区条件: {partition_filter}")
    elif hasattr(table.table_schema, 'partitions') and table.table_schema.partitions:
        # 保持现有自动取最新分区的逻辑（fallback）
        partition_columns = [p.name for p in table.table_schema.partitions]
        # ... 现有逻辑不变
    else:
        logger.info(f"表 {table_name} 不是分区表")

    if limit:
        sql += f" LIMIT {limit}"

    return sql
```

#### 2.3.2 `migrator.py`

**`DataMigrator.__init__` 增加 `partition_filter` 参数：**

```python
def __init__(self, ..., partition_filter: Optional[str] = None):
    # ... 现有逻辑
    self.partition_filter = partition_filter
```

**`_migrate_table_data` 传递 `partition_filter`：**

```python
def _migrate_table_data(self, ...):
    # 仅当源为 MaxCompute 时传递 partition_filter
    kwargs = {'batch_size': batch_size}
    if self.source_type == 'maxcompute' and self.partition_filter:
        kwargs['partition_filter'] = self.partition_filter

    data_iterator = self.source_client.get_table_data(
        source_table_name, **kwargs
    )
    # ... 其余逻辑不变
```

#### 2.3.3 `cli.py`

**新增 CLI 选项：**

```python
@click.option('--partition-filter',
              default=None,
              help='MaxCompute分区过滤条件，如: "pt=\'20240501\'" 或 '
                   '"pt>=\'20240401\' AND pt<=\'20240501\'"。'
                   '设为 "*" 查询所有分区。仅当源为MaxCompute分区表时生效。')
```

**在 `main()` 中将参数传递给 `DataMigrator`：**

```python
migrator = DataMigrator(
    ...,
    partition_filter=final_args.get("partition_filter")
)
```

#### 2.3.4 `config_loader.py`

**`normalize_config` 中解析 `source.partition_filter`：**

```python
# source 段增加
if "partition_filter" in source:
    out.setdefault("partition_filter", source.get("partition_filter"))
```

**`merge_with_cli_and_env` 中增加合并：**

```python
pick("partition_filter", cli_args.get("partition_filter"), env_val=None)
```

#### 2.3.5 JSON 配置文件支持

在 `source` 段中支持 `partition_filter` 字段：

```json
{
  "source": {
    "type": "maxcompute",
    "project_id": "bybest",
    "table_name": "ads_ls_stock_advice_dim",
    "partition_filter": "pt='20240501'"
  },
  "destination": {
    "type": "mysql",
    "...": "..."
  }
}
```

### 2.4 dry-run 模式输出增强

在 `cli.py` 的 `_dry_run` 函数中，增加分区条件的展示：

```python
if migrator.partition_filter:
    click.echo(f"   分区过滤条件: {migrator.partition_filter}")
else:
    click.echo(f"   分区策略: 自动选择最新分区")
```

### 2.5 参数优先级

与现有参数一致，遵循 **CLI > 配置文件 > 自动推断** 的优先级：

1. `--partition-filter` CLI 参数最高优先
2. 配置文件中 `source.partition_filter` 次之
3. 均未设置时，保持现有自动取最新分区的行为

---

## 3. 使用示例

### 3.1 CLI 方式

```bash
# 查询指定日分区
python main.py \
  --source-type maxcompute \
  --source-project-id bybest \
  --source-table-name ads_ls_stock_advice_dim \
  --partition-filter "pt='20240501'" \
  --destination-type mysql \
  ...

# 查询分区范围
python main.py \
  --partition-filter "pt>='20240401' AND pt<='20240430'" \
  -f local_conf/conf.json

# 查询所有分区
python main.py \
  --partition-filter "*" \
  -f local_conf/conf.json
```

### 3.2 配置文件方式

```json
{
  "source": {
    "type": "maxcompute",
    "project_id": "bybest",
    "table_name": "ads_ls_stock_advice_dim",
    "partition_filter": "pt='20240501'"
  },
  "destination_type": "mysql",
  "mysql_dest_host": "localhost",
  "mysql_dest_database": "supplychain_system",
  "destination_table_name": "ads_ls_stock_advice_dim",
  "mode": "overwrite"
}
```

---

## 4. 安全考虑

- `partition_filter` 直接拼接到 SQL 中，在内网使用场景下风险可控
- 如果未来需要防范 SQL 注入，可增加白名单校验：仅允许分区列名、比较运算符和字面量值
- `*` 查询全量数据可能导致耗时过长或 OOM，建议在文档中提示用户注意数据量

---

## 5. 兼容性

- 所有改动均为增量，`partition_filter` 默认为 `None`
- 未设置时行为与改动前完全一致（自动取最新分区）
- 不影响 MySQL 作为数据源的流程（`partition_filter` 仅在 MaxCompute 源时生效）
