# 将MySQL添加为数据源的改造方案

本文档详细说明了为`data-warehouse-migrate`项目添加MySQL作为新的数据迁移来源的设计思路、需要改动的文件、具体改动内容以及后续步骤。

## 1. 设计思路

为了支持MySQL作为新的数据源，并为未来支持更多数据源（如PostgreSQL、SQL Server等）打下基础，我们将对现有架构进行扩展，而不是简单地在现有代码中添加`if/else`分支。

核心设计思路如下：

1.  **引入源客户端接口 (Source Client Interface)**: 定义一个统一的源客户端接口（或基类），包含所有数据源客户端必须实现的方法，如连接、获取表结构、分批读取数据等。这将解耦核心迁移逻辑与具体的数据源实现。
2.  **实现MySQL客户端**: 创建一个新的`MySQLClient`类，实现上述接口，专门负责与MySQL数据库的交互。
3.  **工厂模式创建客户端**: 在`DataMigrator`或`CLI`层面，使用工厂模式或简单的条件判断，根据用户指定的`source-type`（`maxcompute`或`mysql`）来创建相应的客户端实例（`MaxComputeClient`或`MySQLClient`）。
4.  **扩展配置和命令行**: 修改配置文件和命令行接口，以接收MySQL的连接参数。
5.  **扩展表结构映射**: 在`SchemaMapper`中添加新的方法，用于将MySQL的数据类型映射到BigQuery的数据类型。

通过这种方式，`DataMigrator`的核心迁移逻辑将保持不变，它只与源客户端接口交互，而无需关心底层是MaxCompute还是MySQL，从而提高了代码的可维护性和可扩展性。

## 2. 需要改动的文件及改动目的

| 文件路径                                               | 主要改动目的                                                              |
| ------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pyproject.toml`                                       | 添加MySQL驱动依赖（例如 `mysql-connector-python`）。                      |
| `README.md`                                            | 更新文档，说明如何配置和使用MySQL作为数据源。                             |
| `data_warehouse_migrate/config.py`                     | 添加从环境变量加载MySQL连接配置的功能。                                   |
| `data_warehouse_migrate/cli.py`                        | 添加新的命令行参数以支持选择数据源和配置MySQL连接。                       |
| `data_warehouse_migrate/schema_mapper.py`              | 添加MySQL到BigQuery的数据类型映射逻辑。                                   |
| `data_warehouse_migrate/migrator.py`                   | 改造`DataMigrator`以使用源客户端接口，解耦与具体数据源的依赖。            |
| `data_warehouse_migrate/maxcompute_client.py`          | （可选）可进行微调，以符合统一的源客户端接口。                            |

## 3. 新增文件

| 文件路径                                     | 主要目的                                                                    |
| -------------------------------------------- | --------------------------------------------------------------------------- |
| `data_warehouse_migrate/mysql_client.py`     | 实现与MySQL数据库的所有交互，包括连接、获取表结构、分批读取数据等。         |

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

    # MySQL Configuration
    mysql_host: Optional[str] = os.getenv("MYSQL_HOST")
    mysql_user: Optional[str] = os.getenv("MYSQL_USER")
    mysql_password: Optional[str] = os.getenv("MYSQL_PASSWORD")
    mysql_database: Optional[str] = os.getenv("MYSQL_DATABASE")
    mysql_port: int = int(os.getenv("MYSQL_PORT", 3306))
```

### 4.3 `data_warehouse_migrate/cli.py`

修改命令行接口，添加`--source-type`和MySQL相关参数。

```python
# data_warehouse_migrate/cli.py

@click.command()
@click.option('--source-type',
              type=click.Choice(['maxcompute', 'mysql'], case_sensitive=False),
              default='maxcompute',
              help='数据源类型: maxcompute 或 mysql')
# ... (existing options)

# Add MySQL specific options
@click.option('--mysql-host', help='MySQL 主机 (可通过环境变量 MYSQL_HOST 设置)')
@click.option('--mysql-user', help='MySQL 用户名 (可通过环境变量 MYSQL_USER 设置)')
@click.option('--mysql-password', help='MySQL 密码 (可通过环境变量 MYSQL_PASSWORD 设置)')
@click.option('--mysql-database', help='MySQL 数据库 (可通过环境变量 MYSQL_DATABASE 设置)')
@click.option('--mysql-port', type=int, help='MySQL 端口 (可通过环境变量 MYSQL_PORT 设置)')

def main(...):
    # ...
    # 根据 source-type 创建不同的 migrator
    if source_type == 'mysql':
        # ... (validate mysql config)
        migrator = DataMigrator(
            source_type='mysql',
            # ... (pass mysql configs)
        )
    else: # maxcompute
        # ... (validate maxcompute config)
        migrator = DataMigrator(
            source_type='maxcompute',
            # ... (pass maxcompute configs)
        )
    # ...
```

### 4.4 `data_warehouse_migrate/mysql_client.py` (新文件)

创建一个新文件来实现`MySQLClient`。

```python
# data_warehouse_migrate/mysql_client.py

import mysql.connector
import pandas as pd
from typing import Dict, Any, List

class MySQLClient:
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

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        # 使用 `DESCRIBE` 或查询 `information_schema`
        # 返回一个包含列名和数据类型的字典列表
        # 例如: [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'varchar(255)'}]
        pass

    def read_table_in_batches(self, table_name: str, batch_size: int):
        # 使用 pandas.read_sql_query 和 chunksize 参数来分批读取数据
        query = f"SELECT * FROM {table_name}"
        self.connect()
        for chunk in pd.read_sql_query(query, self.conn, chunksize=batch_size):
            yield chunk
        self.close()

    def validate_table_access(self, table_name: str) -> bool:
        # 检查表是否存在
        pass

    def _test_connection(self):
        # 测试连接
        pass
```

### 4.5 `data_warehouse_migrate/schema_mapper.py`

添加MySQL到BigQuery的类型映射。

```python
# data_warehouse_migrate/schema_mapper.py

from google.cloud.bigquery import SchemaField

class SchemaMapper:
    # ... (existing maxcompute mapping)

    def convert_mysql_to_bigquery_schema(self, mysql_schema: list) -> list:
        bigquery_schema = []
        for column in mysql_schema:
            column_name = column['name']
            mysql_type = column['type'].lower()
            
            # 映射逻辑
            if 'int' in mysql_type:
                bq_type = 'INT64'
            elif 'char' in mysql_type or 'text' in mysql_type:
                bq_type = 'STRING'
            elif 'decimal' in mysql_type or 'numeric' in mysql_type:
                bq_type = 'NUMERIC'
            elif 'float' in mysql_type or 'double' in mysql_type:
                bq_type = 'FLOAT64'
            elif 'date' in mysql_type:
                bq_type = 'DATE'
            elif 'datetime' in mysql_type:
                bq_type = 'DATETIME'
            elif 'timestamp' in mysql_type:
                bq_type = 'TIMESTAMP'
            elif 'bool' in mysql_type or 'boolean' in mysql_type:
                bq_type = 'BOOL'
            else:
                bq_type = 'STRING' # 默认
            
            bigquery_schema.append(SchemaField(column_name, bq_type))
        return bigquery_schema
```

### 4.6 `data_warehouse_migrate/migrator.py`

重构`DataMigrator`以支持不同的源客户端。

```python
# data_warehouse_migrate/migrator.py

from .maxcompute_client import MaxComputeClient
from .mysql_client import MySQLClient # 新增

class DataMigrator:
    def __init__(self, source_type: str, destination_project_id: str, **kwargs):
        self.source_client = self._create_source_client(source_type, **kwargs)
        self.bigquery_client = BigQueryClient(...)
        self.schema_mapper = SchemaMapper()
        # ...

    def _create_source_client(self, source_type: str, **kwargs):
        if source_type == 'mysql':
            return MySQLClient(
                host=kwargs.get('mysql_host'),
                user=kwargs.get('mysql_user'),
                # ...
            )
        elif source_type == 'maxcompute':
            return MaxComputeClient(
                # ...
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def migrate_table(self, ...):
        # ...
        # 使用 self.source_client 来获取 schema 和数据
        source_schema = self.source_client.get_table_schema(...)
        
        if isinstance(self.source_client, MySQLClient):
            bigquery_schema = self.schema_mapper.convert_mysql_to_bigquery_schema(source_schema)
        else: # MaxCompute
            bigquery_schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(source_schema)
        
        # ...
        
        for batch_df in self.source_client.read_table_in_batches(...):
            # ...
```

## 5. 后续步骤

1.  **实现代码**: 根据上述方案完成所有代码的编写和修改。
2.  **编写单元测试**: 为`MySQLClient`和`SchemaMapper`中新的MySQL相关功能编写单元测试。
3.  **集成测试**: 进行端到端的集成测试，使用一个真实的MySQL数据库和一个BigQuery实例来验证迁移过程是否成功。
4.  **更新文档**: 完善`README.md`中的使用示例和配置说明。

这个方案提供了一个清晰的、可扩展的路径来为项目添加MySQL支持。
