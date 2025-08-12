# 将MySQL添加为目标数据源的改造方案

本文档详细说明了为`data-warehouse-migrate`项目添加MySQL作为新的数据迁移**目标**的设计思路、需要改动的文件、具体改动内容以及后续步骤。

## 1. 设计思路

为了支持MySQL作为新的数据迁移目标，并为未来支持更多目标数据源（如PostgreSQL、SQL Server等）打下基础，我们将对现有架构进行扩展，而不是简单地在现有代码中添加`if/else`分支。

核心设计思路如下：

1.  **引入目标客户端接口 (Destination Client Interface)**: 定义一个统一的目标客户端接口（或基类），包含所有目标数据源客户端必须实现的方法，如连接、创建表、写入数据等。这将解耦核心迁移逻辑与具体的目标数据源实现。
2.  **实现MySQL写入客户端**: 创建一个新的`MySQLWriter`类，实现上述接口，专门负责与MySQL数据库的交互，包括创建表和写入数据。
3.  **工厂模式创建客户端**: 在`DataMigrator`或`CLI`层面，使用工厂模式或简单的条件判断，根据用户指定的`destination-type`（`bigquery`或`mysql`）来创建相应的客户端实例（`BigQueryClient`或`MySQLWriter`）。
4.  **扩展配置和命令行**: 修改配置文件和命令行接口，以接收MySQL的连接参数。
5.  **扩展表结构映射**: 在`SchemaMapper`中添加新的方法，用于将MaxCompute的数据类型映射到MySQL的数据类型。

通过这种方式，`DataMigrator`的核心迁移逻辑将保持不变，它只与目标客户端接口交互，而无需关心底层是BigQuery还是MySQL，从而提高了代码的可维护性和可扩展性。

## 2. 需要改动的文件及改动目的

| 文件路径                                               | 主要改动目的                                                              |
| ------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pyproject.toml`                                       | 添加MySQL驱动依赖（例如 `mysql-connector-python`）。                      |
| `README.md`                                            | 更新文档，说明如何配置和使用MySQL作为目标数据源。                         |
| `data_warehouse_migrate/config.py`                     | 添加从环境变量加载MySQL目标连接配置的功能。                               |
| `data_warehouse_migrate/cli.py`                        | 添加新的命令行参数以支持选择目标数据源和配置MySQL连接。                   |
| `data_warehouse_migrate/schema_mapper.py`              | 添加MaxCompute到MySQL的数据类型映射逻辑。                                 |
| `data_warehouse_migrate/migrator.py`                   | 改造`DataMigrator`以使用目标客户端接口，解耦与具体目标数据源的依赖。      |
| `data_warehouse_migrate/bigquery_client.py`            | （可选）可进行微调，以符合统一的目标客户端接口。                          |

## 3. 新增文件

| 文件路径                                     | 主要目的                                                                    |
| -------------------------------------------- | --------------------------------------------------------------------------- |
| `data_warehouse_migrate/mysql_writer.py`     | 实现与MySQL数据库的所有写入交互，包括连接、创建表、写入数据等。             |

## 4. 具体改动内容

### 4.1 `pyproject.toml`

在`[project]`部分的`dependencies`列表中添加MySQL驱动。我们选择`mysql-connector-python`，因为它是官方驱动。

```diff
[project]
...
dependencies = [
    "google-cloud-bigquery>=3.11.0",
    "pyodps>=0.11.0",
    "click>=8.1.0",
    "pandas>=2.0.0",
    "pyarrow>=12.0.0",
    "tqdm>=4.65.0",
    "python-dotenv>=1.0.0",
    "pandas-gbq>=0.26.1",
+   "mysql-connector-python>=8.0.0",
]
...
```

### 4.2 `data_warehouse_migrate/config.py`

在`Config`类中添加MySQL的配置项。

```python
# data_warehouse_migrate/config.py

class Config:
    # ... (existing MaxCompute and BigQuery configs)

    # MySQL Destination Configuration
    mysql_dest_host: Optional[str] = os.getenv("MYSQL_DEST_HOST")
    mysql_dest_user: Optional[str] = os.getenv("MYSQL_DEST_USER")
    mysql_dest_password: Optional[str] = os.getenv("MYSQL_DEST_PASSWORD")
    mysql_dest_database: Optional[str] = os.getenv("MYSQL_DEST_DATABASE")
    mysql_dest_port: int = int(os.getenv("MYSQL_DEST_PORT", 3306))
```

### 4.3 `data_warehouse_migrate/cli.py`

修改命令行接口，添加`--destination-type`和MySQL相关参数。

```python
# data_warehouse_migrate/cli.py

@click.command()
# ... (existing source options)

@click.option('--destination-type',
              type=click.Choice(['bigquery', 'mysql'], case_sensitive=False),
              default='bigquery',
              help='目标数据源类型: bigquery 或 mysql')

# ... (existing BigQuery destination options)

# Add MySQL destination specific options
@click.option('--mysql-dest-host', help='MySQL 目标主机 (可通过环境变量 MYSQL_DEST_HOST 设置)')
@click.option('--mysql-dest-user', help='MySQL 目标用户名 (可通过环境变量 MYSQL_DEST_USER 设置)')
@click.option('--mysql-dest-password', help='MySQL 目标密码 (可通过环境变量 MYSQL_DEST_PASSWORD 设置)')
@click.option('--mysql-dest-database', help='MySQL 目标数据库 (可通过环境变量 MYSQL_DEST_DATABASE 设置)')
@click.option('--mysql-dest-port', type=int, help='MySQL 目标端口 (可通过环境变量 MYSQL_DEST_PORT 设置)')

def main(...):
    # ...
    # 根据 destination-type 创建不同的 migrator
    if destination_type == 'mysql':
        # ... (validate mysql dest config)
        migrator = DataMigrator(
            destination_type='mysql',
            # ... (pass mysql dest configs)
        )
    else: # bigquery
        # ... (validate bigquery config)
        migrator = DataMigrator(
            destination_type='bigquery',
            # ... (pass bigquery configs)
        )
    # ...
```

### 4.4 `data_warehouse_migrate/mysql_writer.py` (新文件)

创建一个新文件来实现`MySQLWriter`。

```python
# data_warehouse_migrate/mysql_writer.py

import mysql.connector
import pandas as pd
from typing import Dict, Any, List

class MySQLWriter:
    def __init__(self, host, user, password, database, port=3306):
        self.connection_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port,
        }
        self.conn = None

    def connect(self):
        if not self.conn or not self.conn.is_connected():
            self.conn = mysql.connector.connect(**self.connection_config)

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def create_table(self, table_name: str, schema: List[Dict[str, Any]], mode: str):
        # 根据 schema 创建 MySQL 表
        # mode 可以是 'overwrite' 或 'append'
        pass

    def write_dataframe(self, table_name: str, dataframe: pd.DataFrame, mode: str):
        # 使用 pandas.to_sql 将 DataFrame 写入 MySQL
        # mode 可以是 'append', 'replace'
        self.connect()
        dataframe.to_sql(name=table_name, con=self.conn, if_exists=mode, index=False)
        self.close()

    def table_exists(self, database: str, table_name: str) -> bool:
        # 检查表是否存在
        pass

    def _test_connection(self):
        # 测试连接
        pass
```

### 4.5 `data_warehouse_migrate/schema_mapper.py`

添加MaxCompute到MySQL的类型映射。

```python
# data_warehouse_migrate/schema_mapper.py

class SchemaMapper:
    # ... (existing maxcompute to bigquery mapping)

    def convert_maxcompute_to_mysql_schema(self, maxcompute_schema: list) -> list:
        mysql_schema = []
        for column in maxcompute_schema:
            column_name = column['name']
            maxcompute_type = column['type'].lower()
            
            # 映射逻辑
            if maxcompute_type in ['bigint', 'int', 'smallint', 'tinyint']:
                mysql_type = 'BIGINT'
            elif maxcompute_type in ['double', 'float']:
                mysql_type = 'DOUBLE'
            elif maxcompute_type == 'decimal':
                mysql_type = 'DECIMAL(18, 4)' # 示例精度，可根据需求调整
            elif maxcompute_type in ['string', 'varchar', 'char']:
                mysql_type = 'VARCHAR(255)' # 示例长度，可根据需求调整
            elif maxcompute_type == 'boolean':
                mysql_type = 'TINYINT(1)'
            elif maxcompute_type == 'datetime':
                mysql_type = 'DATETIME'
            elif maxcompute_type == 'timestamp':
                mysql_type = 'TIMESTAMP'
            elif maxcompute_type == 'date':
                mysql_type = 'DATE'
            elif maxcompute_type == 'binary':
                mysql_type = 'BLOB'
            # 对于 ARRAY, MAP, STRUCT 等复杂类型，可能需要特殊处理或转换为 JSON 字符串
            else:
                mysql_type = 'TEXT' # 默认
            
            mysql_schema.append({'name': column_name, 'type': mysql_type})
        return mysql_schema
```

### 4.6 `data_warehouse_migrate/migrator.py`

重构`DataMigrator`以支持不同的目标客户端。

```python
# data_warehouse_migrate/migrator.py

from .bigquery_client import BigQueryClient
from .mysql_writer import MySQLWriter # 新增

class DataMigrator:
    def __init__(self, source_project_id: str, destination_type: str, **kwargs):
        self.maxcompute_client = MaxComputeClient(...)
        self.destination_client = self._create_destination_client(destination_type, **kwargs)
        self.schema_mapper = SchemaMapper()
        # ...

    def _create_destination_client(self, destination_type: str, **kwargs):
        if destination_type == 'mysql':
            return MySQLWriter(
                host=kwargs.get('mysql_dest_host'),
                user=kwargs.get('mysql_dest_user'),
                # ...
            )
        elif destination_type == 'bigquery':
            return BigQueryClient(
                # ...
            )
        else:
            raise ValueError(f"Unsupported destination type: {destination_type}")

    def migrate_table(self, source_table_name: str, destination_table_name: str, mode: str, batch_size: int):
        # ...
        source_schema = self.maxcompute_client.get_table_schema(source_table_name)
        
        if isinstance(self.destination_client, MySQLWriter):
            destination_schema = self.schema_mapper.convert_maxcompute_to_mysql_schema(source_schema)
            # 创建MySQL表
            self.destination_client.create_table(destination_table_name, destination_schema, mode)
        else: # BigQuery
            destination_schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(source_schema)
            # BigQuery创建表逻辑
            # ...
        
        # ...
        
        for batch_df in self.maxcompute_client.read_table_in_batches(source_table_name, batch_size):
            if isinstance(self.destination_client, MySQLWriter):
                self.destination_client.write_dataframe(destination_table_name, batch_df, mode)
            else: # BigQuery
                # BigQuery写入逻辑
                # ...
```

## 5. 后续步骤

1.  **实现代码**: 根据上述方案完成所有代码的编写和修改。
2.  **编写单元测试**: 为`MySQLWriter`和`SchemaMapper`中新的MySQL相关功能编写单元测试。
3.  **集成测试**: 进行端到端的集成测试，使用一个真实的MaxCompute实例和一个MySQL实例来验证迁移过程是否成功。
4.  **更新文档**: 完善`README.md`中的使用示例和配置说明。

这个方案提供了一个清晰的、可扩展的路径来为项目添加MySQL作为目标数据源的支持。
