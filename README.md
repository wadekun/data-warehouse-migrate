# data-warehouse-migrate

数仓迁移工具，支持从阿里云MaxCompute迁移数据到Google Cloud BigQuery，**现在也支持从MaxCompute迁移数据到MySQL**。

## 功能特性

- 🚀 支持从MaxCompute到BigQuery的数据迁移
- 📊 自动处理表结构转换和数据类型映射
- 🔄 支持两种迁移模式：覆盖(overwrite)和追加(append)
- 📦 支持批量数据处理，避免内存溢出
- 🐳 支持Docker部署
- 📝 完善的日志记录和错误处理
- 🧪 支持试运行模式，验证配置和连接

## 安装

### 本地安装

```bash
# 克隆项目
git clone <repository-url>
cd data-warehouse-migrate

# 安装依赖
pip install -e .
```

### Docker安装

```bash
# 构建镜像
docker build -t data-warehouse-migrate .

# 或使用docker-compose
docker-compose build
```

## 配置

### 环境变量配置

创建 `.env` 文件（参考 `.env.example`）：

```bash
# MaxCompute配置
MAXCOMPUTE_ACCESS_ID=your_access_id
MAXCOMPUTE_SECRET_ACCESS_KEY=your_secret_key
MAXCOMPUTE_ENDPOINT=http://service.cn.maxcompute.aliyun.com/api

# BigQuery配置
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/bigquery-credentials.json

# MySQL目标配置
MYSQL_DEST_HOST=your_mysql_host
MYSQL_DEST_USER=your_mysql_user
MYSQL_DEST_PASSWORD=your_mysql_password
MYSQL_DEST_DATABASE=your_mysql_database
MYSQL_DEST_PORT=3306

# 日志配置
LOG_LEVEL=INFO
```

### BigQuery凭证配置

1. 在Google Cloud Console中创建服务账号
2. 下载服务账号的JSON凭证文件
3. 设置环境变量 `GOOGLE_APPLICATION_CREDENTIALS` 指向该文件

## 使用方法

### 命令行参数

```bash
data-warehouse-migrate [OPTIONS]
```

#### 必需参数

- `--source-project-id`: MaxCompute源项目ID
- `--source-table-name`: MaxCompute源表名
- `--destination-table-name`: 目标表名

#### 可选参数

- `--destination-type`: 目标数据源类型，可选值：`bigquery` 或 `mysql`，默认为 `bigquery`
- `--destination-project-id`: BigQuery目标项目ID (仅当destination-type为bigquery时需要)
- `--destination-dataset-id`: BigQuery目标数据集ID (仅当destination-type为bigquery时需要)
- `--mode`: 迁移模式，可选值：`overwrite`(覆盖) 或 `append`(追加)，默认为 `append`
- `--batch-size`: 批次大小，默认为 `10000`
- `--maxcompute-access-id`: MaxCompute AccessKey ID
- `--maxcompute-secret-key`: MaxCompute AccessKey Secret
- `--maxcompute-endpoint`: MaxCompute Endpoint
- `--bigquery-credentials-path`: BigQuery服务账号凭证文件路径
- `--mysql-dest-host`: MySQL目标主机
- `--mysql-dest-user`: MySQL目标用户名
- `--mysql-dest-password`: MySQL目标密码
- `--mysql-dest-database`: MySQL目标数据库
- `--mysql-dest-port`: MySQL目标端口
- `--log-level`: 日志级别，可选值：`DEBUG`, `INFO`, `WARNING`, `ERROR`，默认为 `INFO`
- `--dry-run`: 试运行模式，只检查连接和表结构，不实际迁移数据

### 使用示例

#### 从MaxCompute迁移到BigQuery

```bash
data-warehouse-migrate \
  --source-project-id my-maxcompute-project \
  --source-table-name user_table \
  --destination-type bigquery \
  --destination-project-id my-bigquery-project \
  --destination-dataset-id analytics \
  --destination-table-name users \
  --mode append
```

#### 从MaxCompute迁移到MySQL

```bash
data-warehouse-migrate \
  --source-project-id my-maxcompute-project \
  --source-table-name orders \
  --destination-type mysql \
  --mysql-dest-host localhost \
  --mysql-dest-user root \
  --mysql-dest-password your_password \
  --mysql-dest-database my_database \
  --destination-table-name orders_from_maxcompute \
  --mode overwrite
```

#### 试运行

```bash
data-warehouse-migrate --dry-run [其他参数...]
```

### 配置文件支持

- 支持通过配置文件加载迁移参数，命令行示例：

```bash
data-warehouse-migrate -f conf.json
```

- 参数优先级：命令行 > 配置文件 > 环境变量。
- 配置文件（JSON）示例（扁平键与 CLI 一致）：

```json
{
  "source_project_id": "bybest",
  "source_table_name": "ods_lm_spu",
  "destination_type": "mysql",
  "mysql_dest_host": "127.0.0.1",
  "mysql_dest_port": 3306,
  "mysql_dest_user": "root",
  "mysql_dest_password": "${MYSQL_PASSWORD}",
  "mysql_dest_database": "supplychain_system",
  "destination_table_name": "lm_spu",
  "mode": "overwrite",
  "batch_size": 100000,
  "log_level": "INFO",
  "dry_run": false,
  "preserve_string_null_tokens": true,
  "string_null_tokens": ["nan","None","null","<NA>","NaN"],
  "null_on_non_nullable": "fail",
  "null_fill_sentinel": ""
}
```

- 也支持分组键写法（会自动规范化为扁平键）：

```json
{
  "source": {"project_id": "bybest", "table_name": "ods_lm_spu"},
  "destination": {
    "type": "mysql",
    "table_name": "lm_spu",
    "mysql": {"host": "127.0.0.1", "port": 3306, "user": "root", "password": "${MYSQL_PASSWORD}", "database": "supplychain_system"}
  },
  "run": {"mode": "overwrite", "batch_size": 100000, "log_level": "INFO", "dry_run": false},
  "compat": {"preserve_string_null_tokens": true, "string_null_tokens": ["nan","None","null","<NA>","NaN"], "null_on_non_nullable": "fail", "null_fill_sentinel": ""}
}
```

### 字段映射（仅 MySQL）

- 仅当 `destination_type` 为 `mysql` 时启用映射；BigQuery 默认忽略该配置（为未来扩展预留）。
- 支持能力：
  - include/exclude：列选择
  - rename：列重命名（源→目标，大小写不敏感）
  - type_override：覆盖目标列类型（MySQL 类型字面量）
  - defaults：写入前 DataFrame 层默认值填充
  - computed：派生列（白名单函数：concat/upper/lower/substr/NOW）
  - order：最终输出列顺序

- 配置文件示例中的 `mappings` 段：

```json
{
  "mappings": {
    "default": {
      "exclude": ["pt"],
      "rename": {"sku_code": "sku"},
      "type_override": {"sku": "VARCHAR(64)"},
      "defaults": {"deleted": "b'0'"},
      "computed": {"skc_code": {"func": "concat", "args": ["spu_code", "-", "size"]}},
      "order": ["id", "sku", "skc_code", "deleted", "create_time"]
    },
    "tables": [
      {"source_table": "ods_lm_spu", "exclude": ["pt"], "rename": {"sku_sabc": "sku_grade"}, "type_override": {"sku_grade": "VARCHAR(8)"}}
    ]
  }
}
```

- 应用顺序：类型应用后 → 列选择 → 重命名 → 计算列 → 应用层默认值 → 按顺序重排 → 数据库默认值/非空校验 → 写入。
- 校验：
  - 源列必须存在；重命名后的目标列不可重复
  - computed 仅支持白名单函数；不执行任意表达式
  - 未配置 mappings 时行为不变。

#### computed: format 模板函数（统一格式拼接）

- 作用：使用模板 + 格式化生成新列（仅 MySQL 映射路径）。支持零填充等常见格式需求。
- 支持两种写法：
  - 命名占位符（推荐）：

    ```json
    {
      "computed": {
        "year_week": { "func": "format", "args": ["{year}-{week:02d}"] }
      }
    }
    ```

  - 位置占位符：

    ```json
    {
      "computed": {
        "year_week": { "func": "format", "args": ["{}-{:02d}", "year", "week"] }
      }
    }
    ```

- 示例（year=2024, week=9/10/11 → 2024-09/2024-10/2024-11）：

  ```json
  {
    "computed": {
      "year_week": { "func": "format", "args": ["{year}-{week:02d}"] }
    }
  }
  ```

- 规则与说明：
  - 命名占位符中的字段名使用“映射后的列名”（即在 rename/computed 后可见的列名）。
  - 对数字格式（如 :02d），None/NaN/空字符串按 0 处理（week=None → 00）。
  - 非数字格式的 None/NaN 作为空字符串处理。
  - 若需先重命名再格式化，format 读取的应是重命名后的列名。
  - 模板/参数错误不会中断迁移，该列将生成为空字符串，并在日志中输出 debug 信息。

#### computed 可用函数一览

以下函数仅在 MySQL 映射路径生效，均为白名单函数，按映射阶段的“计算列（computed）”顺序依次执行：

- concat: 字符串拼接
  - 说明：将参数依次拼接，参数可为列名或字符串字面量
  - 示例：
    ```json
    { "computed": { "sku_full": { "func": "concat", "args": ["spu_code", "-", "size"] } } }
    ```

- upper: 转大写
  - 说明：将指定列转为大写字符串
  - 示例：
    ```json
    { "computed": { "sku_upper": { "func": "upper", "args": ["sku_code"] } } }
    ```

- lower: 转小写
  - 说明：将指定列转为小写字符串
  - 示例：
    ```json
    { "computed": { "sku_lower": { "func": "lower", "args": ["sku_code"] } } }
    ```

- substr: 子串
  - 说明：从指定列的字符串取子串；参数为列名、起始下标（从0开始）、长度（可省略表示到末尾）
  - 示例：
    ```json
    { "computed": { "sku_prefix": { "func": "substr", "args": ["sku_code", 0, 3] } } }
    ```

- now: 当前时间（UTC）
  - 说明：生成当前 UTC 时间戳（pandas Timestamp），常用于补充时间列
  - 示例：
    ```json
    { "computed": { "create_time": { "func": "now", "args": [] } } }
    ```

- format: 模板+格式化（推荐统一输出格式，支持零填充）
  - 说明：见上节“computed: format 模板函数（统一格式拼接）”
  - 命名占位符（推荐）：
    ```json
    { "computed": { "year_week": { "func": "format", "args": ["{year}-{week:02d}"] } } }
    ```
  - 位置占位符：
    ```json
    { "computed": { "year_week": { "func": "format", "args": ["{}-{:02d}", "year", "week"] } } }
    ```

注意：
- computed 的键为“目标列名”；若与现有列重名，将覆盖该列（建议避免与 rename 目标重名）。
- 计算顺序按配置文件中出现的先后依次执行；后续 computed 可以引用前面刚生成的列。
- 对 None/NaN 的处理：
  - concat/upper/lower/substr：内部会将列转为字符串；None/NaN 结果通常为空字符串（或依赖 pandas 的转换规则）
  - format：若使用数字格式（如 :02d），None/NaN 会被当作 0 处理；非数字格式当作空字符串处理


### Docker使用

#### 使用docker run

```bash
docker run --rm \
  -e MAXCOMPUTE_ACCESS_ID=your_access_id \
  -e MAXCOMPUTE_SECRET_ACCESS_KEY=your_secret_key \
  -e MAXCOMPUTE_ENDPOINT=http://service.cn.maxcompute.aliyun.com/api \
  -v /path/to/bigquery-credentials.json:/app/credentials/bigquery-credentials.json:ro \
  data-warehouse-migrate \
  --source-project-id my-maxcompute-project \
  --source-table-name user_table \
  --destination-project-id my-bigquery-project \
  --destination-dataset-id analytics \
  --destination-table-name users \
  --mode append
```

#### 使用docker-compose

1. 修改 `docker-compose.yml` 中的环境变量和命令参数
2. 运行：

```bash
docker-compose up
```

## 数据类型映射

### MaxCompute -> BigQuery

工具会自动处理MaxCompute和BigQuery之间的数据类型转换：

| MaxCompute类型 | BigQuery类型 |
|---------------|-------------|
| bigint, int, smallint, tinyint | INT64 |
| double, float | FLOAT64 |
| decimal | NUMERIC |
| string, varchar, char | STRING |
| boolean | BOOL |
| datetime | DATETIME |
| timestamp | TIMESTAMP |
| date | DATE |
| binary | BYTES |
| array<T> | REPEATED T |
| map<K,V> | RECORD |
| struct | RECORD |

### MaxCompute -> MySQL

工具会自动处理MaxCompute和MySQL之间的数据类型转换：

| MaxCompute类型 | MySQL类型 |
|---------------|-------------|
| bigint, int, smallint, tinyint | BIGINT |
| double, float | DOUBLE |
| decimal | DECIMAL(18, 4) |
| string, varchar, char | VARCHAR(255) |
| boolean | TINYINT(1) |
| datetime | DATETIME |
| timestamp | TIMESTAMP |
| date | DATE |
| binary | BLOB |
| array<T>, map<K,V>, struct | TEXT (复杂类型可能需要手动调整) |

## 分区表处理

工具自动处理MaxCompute分区表：

1. **自动检测分区表**：工具会自动检测源表是否为分区表
2. **智能分区选择**：
   - 优先查找 `pt` 分区字段，使用最新分区数据
   - 如果没有 `pt` 字段，使用所有分区字段的最新值
   - 自动构建带分区条件的查询SQL，避免全表扫描错误
3. **分区字段处理**：分区字段不会在BigQuery目标表中创建，只用于数据筛选

## 数据类型兼容性处理

工具采用**基于源表结构的类型处理**机制，确保数据类型的准确性：

1. **基于源表结构的类型转换**（推荐）：
   - **优先使用源表定义的数据类型**，避免错误的类型推断
   - 根据MaxCompute表结构中的类型定义进行精确转换
   - 确保字符串类型字段（如状态码）不会被错误转换为数值类型
   - 示例：`offline_pay_confirm_status` 在源表中定义为 `string` 类型，即使包含 `'0'`, `'1'` 等值也保持字符串类型

2. **智能数据清理**：
   - 根据源表类型进行相应的数据清理和验证
   - 处理NULL值和特殊值（如无穷大、NaN）
   - 转换布尔值的不同表示形式

3. **PyArrow兼容性**：
   - 自动处理pandas nullable类型（Int64, boolean等）与pyarrow的兼容性问题
   - 智能转换有NaN值的整数列为float64类型
   - 确保所有数据类型都能正确传输到BigQuery

4. **错误处理和回退机制**：
   - 详细的类型转换错误分析
   - 当源表结构获取失败时，自动回退到基础数据清理
   - 提供具体的错误定位信息

### 类型转换规则

| MaxCompute源表类型 | 转换规则 | BigQuery目标类型 |
|------------------|---------|-----------------|
| string, varchar, char | 保持字符串，清理特殊值 | STRING |
| bigint, int, smallint, tinyint | 转换为数值，有NaN时用float64 | INT64/FLOAT64 |
| double, float, decimal | 转换为浮点数 | FLOAT64 |
| boolean | 转换布尔值表示 | BOOL |
| datetime, timestamp, date | 保持原格式 | DATETIME/TIMESTAMP/DATE |

## 开发

### 运行测试

```bash
# 安装测试依赖
pip install pytest

# 运行测试
pytest tests/
```

### 项目结构

```
data-warehouse-migrate/
├── data_warehouse_migrate/     # 主要代码
│   ├── __init__.py
│   ├── cli.py                 # 命令行接口
│   ├── config.py              # 配置管理
│   ├── exceptions.py          # 自定义异常
│   ├── logger.py              # 日志配置
│   ├── schema_mapper.py       # 数据类型映射
│   ├── maxcompute_client.py   # MaxCompute客户端
│   ├── bigquery_client.py     # BigQuery客户端
│   ├── mysql_writer.py        # MySQL写入客户端
│   └── migrator.py            # 核心迁移逻辑
├── tests/                     # 测试文件
├── Dockerfile                 # Docker配置
├── docker-compose.yml         # Docker Compose配置
├── pyproject.toml            # 项目配置
└── README.md                 # 说明文档
```

## 注意事项

1. **权限要求**：
   - MaxCompute：需要有读取源表的权限
   - BigQuery：需要有创建数据集和表、写入数据的权限

2. **数据量考虑**：
   - 大表迁移建议使用较小的批次大小
   - 可以通过 `--batch-size` 参数调整批次大小

3. **网络连接**：
   - 确保网络连接稳定，大数据量迁移可能需要较长时间

4. **成本考虑**：
   - BigQuery按查询和存储收费，请注意成本控制

## 故障排除

### 常见问题

1. **连接失败**：
   - 检查网络连接
   - 验证凭证配置
   - 确认项目ID和权限

2. **数据类型转换错误**：
   - 查看日志中的详细错误信息
   - 检查源表的数据类型定义

3. **内存不足**：
   - 减小批次大小 (`--batch-size`)
   - 使用更大内存的机器

### 日志分析

使用 `--log-level DEBUG` 获取详细的调试信息：

```bash
data-warehouse-migrate --log-level DEBUG [其他参数...]
```

## 许可证

[添加许可证信息]

## 贡献

欢迎提交Issue和Pull Request！