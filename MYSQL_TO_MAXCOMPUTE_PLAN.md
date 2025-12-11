# MySQL 到 MaxCompute 双向同步改造方案

本文档详细说明了将现有 `data-warehouse-migrate` 工具从单纯的 "MaxCompute -> BigQuery/MySQL" 扩展为支持 "MySQL -> MaxCompute" 的双向迁移工具的改造方案。

## 1. 设计思路

核心目标是对现有架构进行**泛化（Generalization）**，将“源（Source）”和“目标（Destination）”的概念解耦，不再硬编码 MaxCompute 为源。

### 1.1 抽象接口

引入通用的 `SourceClient` 和 `DestinationClient` 概念（可以是显式的接口类或鸭子类型约定）。

- **Source Client 接口能力**:
    - `test_connection()`: 测试连接有效性。
    - `validate_table_access(table_name)`: 验证源表是否存在且可读。
    - `get_table_schema(table_name)`: 获取统一格式的表结构（列名、类型、注释等）。
    - `get_table_data(table_name, batch_size)`: 返回数据生成器 (`Iterator[pd.DataFrame]`)。

- **Destination Client 接口能力**:
    - `test_connection()`: 测试连接有效性。
    - `table_exists(table_name)`: 检查目标表是否存在。
    - `create_table(table_name, schema, mode)`: 根据 Schema 创建表。
    - `truncate_table(table_name)`: 清空表数据。
    - `write_dataframe(table_name, df, mode)`: 将 DataFrame 写入目标表。

## 2. 模块改造计划

### 2.1 MySQL 客户端增强 (`data_warehouse_migrate/mysql_client.py`)

将现有的 `mysql_writer.py` 重命名或重构为 `mysql_client.py`，使其既能读也能写。

- **新增读能力**:
    - 实现 `get_table_data`: 使用 `pd.read_sql(sql, con=engine, chunksize=batch_size)` 实现流式读取，避免内存溢出。
    - 实现 `validate_table_access`: 执行简单的 `SELECT 1 FROM table LIMIT 1`。
- **复用**:
    - 现有的 `get_table_schema` 已经实现，可直接作为源结构获取方法。

### 2.2 MaxCompute 客户端增强 (`data_warehouse_migrate/maxcompute_client.py`)

为现有的 `MaxComputeClient` 补充写入能力。

- **新增写能力**:
    - 实现 `table_exists`: 检查 ODPS 表是否存在。
    - 实现 `create_table`: 根据通用 Schema 拼接 ODPS DDL (`CREATE TABLE ...`)。需处理类型转换（如 MySQL `VARCHAR` -> MaxCompute `STRING`）。
    - 实现 `truncate_table`: 执行 `TRUNCATE TABLE`。
    - 实现 `write_dataframe`: 使用 PyODPS 的 `open_writer` 接口或 `odps.write_table` 方法将 Pandas DataFrame 写入 MaxCompute。

### 2.3 Schema Mapper 扩展 (`data_warehouse_migrate/schema_mapper.py`)

新增反向类型映射逻辑。

- **新增方法**: `convert_mysql_to_maxcompute_schema(mysql_schema)`
- **映射规则参考**:
    - `BIGINT`, `INT` -> `BIGINT`
    - `VARCHAR`, `CHAR`, `TEXT`, `LONGTEXT` -> `STRING`
    - `DATETIME`, `TIMESTAMP` -> `DATETIME`
    - `DOUBLE`, `FLOAT` -> `DOUBLE`
    - `DECIMAL` -> `DECIMAL`
    - `TINYINT` -> `BIGINT` (或视情况转 `BOOLEAN`)

### 2.4 迁移器逻辑泛化 (`data_warehouse_migrate/migrator.py`)

- **构造函数调整**: 增加 `source_type` 参数（默认为 `maxcompute`，新增 `mysql`）。
- **工厂方法**: 增加 `_create_source_client`，根据 `source_type` 实例化 `MaxComputeClient` 或 `MySQLClient`。
- **逻辑替换**: 将 `migrate_table` 中所有硬编码的 `self.maxcompute_client` 调用替换为 `self.source_client`。

### 2.5 CLI 与配置 (`data_warehouse_migrate/cli.py` & `config.py`)

- **新增参数**:
    - `--source-type`: 支持 `maxcompute` 或 `mysql`。
    - **MySQL 源配置**: 为避免与目标配置混淆，建议引入明确的源配置参数，如 `--source-mysql-host`, `--source-mysql-user` 等（或者在代码逻辑中复用 `--mysql-dest-*` 参数，但需在文档中明确说明当 `source-type=mysql` 时这些参数用于源）。为清晰起见，推荐分离或使用前缀区分。

## 3. 执行步骤

1.  **Client 重构**:
    - 改造 `mysql_writer.py` -> `mysql_client.py`，实现读取接口。
    - 增强 `maxcompute_client.py`，实现写入接口。
2.  **Schema 映射**:
    - 在 `schema_mapper.py` 中添加 MySQL -> MaxCompute 的映射代码。
3.  **Migrator 改造**:
    - 修改 `DataMigrator` 类，引入 Source Client 抽象。
4.  **CLI 适配**:
    - 更新 `cli.py` 以支持 `--source-type` 及相关参数解析。
5.  **验证**:
    - 编写测试脚本验证 MySQL -> MaxCompute 的流程。

此方案实施后，工具将具备双向迁移能力，为后续支持更多数据源（如 PostgreSQL）打下坚实基础。
