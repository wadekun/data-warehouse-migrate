# MySQL到MaxCompute数据同步方案设计

## 1. 项目背景

现有数仓迁移工具已支持从MaxCompute迁移数据到BigQuery和MySQL。为了满足数据回流和多向同步的需求，需要实现从MySQL到MaxCompute的数据同步功能。

## 2. 设计原则

1. **最小侵入性**：尽量复用现有代码架构，减少对现有功能的修改
2. **双向扩展**：为未来实现任意数据源之间的同步奠定基础
3. **一致性**：保持与现有API和配置格式的统一
4. **高性能**：支持大数据量的批量同步和增量同步

## 3. 架构设计

### 3.1 现有架构分析

```
现有数据流：
MaxCompute (源) → SchemaMapper → BigQuery/MySQL (目标)

组件职责：
- MaxComputeClient: 读取MaxCompute数据
- BigQueryClient/MySQLWriter: 写入目标数据
- SchemaMapper: 数据类型映射转换
- DataMigrator: 协调整个迁移过程
```

### 3.2 扩展后架构

```
新数据流：
MySQL (源) → SchemaMapper → MaxCompute (目标)

支持的数据流方向：
1. MaxCompute → BigQuery (现有)
2. MaxCompute → MySQL (现有)
3. MySQL → MaxCompute (新增)
```

## 4. 实现方案

### 4.1 新增组件

#### 4.1.1 MySQLReader类

**文件路径**: `data_warehouse_migrate/mysql_reader.py`

```python
"""
MySQL数据读取客户端
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Iterator
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

class MySQLReader:
    """MySQL数据读取器"""

    def __init__(self, host: str, user: str, password: str,
                 database: str, port: int = 3306):
        """
        初始化MySQL读取器

        Args:
            host: MySQL主机地址
            user: 用户名
            password: 密码
            database: 数据库名
            port: 端口号
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.engine = self._create_engine()

    def _create_engine(self):
        """创建SQLAlchemy引擎"""
        encoded_password = quote_plus(self.password)
        connection_string = (
            f"mysql+mysqlconnector://{self.user}:{encoded_password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string)

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"MySQL连接失败: {e}")
            return False

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表结构

        Returns:
            List[Dict]: 列信息列表
        """
        query = """
            SELECT
                COLUMN_NAME as name,
                DATA_TYPE as type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                CHARACTER_MAXIMUM_LENGTH as max_length,
                NUMERIC_PRECISION as numeric_precision,
                NUMERIC_SCALE as numeric_scale,
                COLUMN_COMMENT as comment
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "database": self.database,
                "table_name": table_name
            }).fetchall()

            columns = []
            for row in result:
                # 构建类型字符串，包含精度信息
                type_str = row.type
                if row.type in ['varchar', 'char'] and row.max_length:
                    type_str = f"{row.type}({row.max_length})"
                elif row.type in ['decimal', 'numeric'] and row.numeric_precision:
                    scale = row.numeric_scale if row.numeric_scale else 0
                    type_str = f"{row.type}({row.numeric_precision},{scale})"

                columns.append({
                    'name': row.name,
                    'type': type_str.lower(),
                    'is_nullable': row.is_nullable == 'YES',
                    'default': row.column_default,
                    'comment': row.comment or ''
                })

            return columns

    def get_table_data(self, table_name: str,
                      batch_size: int = 10000,
                      columns: Optional[List[str]] = None) -> Iterator[pd.DataFrame]:
        """
        流式读取表数据

        Args:
            table_name: 表名
            batch_size: 批次大小
            columns: 指定列名列表，None表示所有列

        Yields:
            pd.DataFrame: 批次数据
        """
        # 构建列名
        col_str = '*' if not columns else ', '.join(f"`{c}`" for c in columns)

        # 构建查询SQL
        query = f"SELECT {col_str} FROM `{table_name}`"

        # 使用pandas的chunksize进行分批读取
        try:
            for chunk in pd.read_sql(
                query,
                self.engine,
                chunksize=batch_size,
                index_col=None
            ):
                yield chunk
        except Exception as e:
            logger.error(f"读取数据失败: {e}")
            raise

    def get_row_count(self, table_name: str) -> int:
        """获取表行数"""
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
            return result
```

#### 4.1.2 MaxComputeWriter类

**文件路径**: `data_warehouse_migrate/maxcompute_writer.py`

```python
"""
MaxCompute数据写入客户端
"""

from typing import List, Dict, Any
from odps import ODPS
from odps.df import DataFrame
import pandas as pd

class MaxComputeWriter:
    """MaxCompute数据写入器"""

    def __init__(self, access_id: str, secret_access_key: str,
                 endpoint: str, project: str):
        """
        初始化MaxCompute写入器

        Args:
            access_id: AccessKey ID
            secret_access_key: AccessKey Secret
            endpoint: MaxCompute endpoint
            project: 项目名
        """
        self.access_id = access_id
        self.secret_access_key = secret_access_key
        self.endpoint = endpoint
        self.project = project

        self._odps = None

    @property
    def odps(self) -> ODPS:
        """获取ODPS实例"""
        if self._odps is None:
            self._odps = ODPS(
                self.access_id,
                self.secret_access_key,
                self.project,
                self.endpoint
            )
        return self._odps

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            # 尝试列出项目
            list(self.odps.list_tables())
            return True
        except Exception as e:
            logger.error(f"MaxCompute连接失败: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            self.odps.get_table(table_name)
            return True
        except:
            return False

    def create_table(self, table_name: str,
                    schema: List[Dict[str, Any]],
                    comment: str = '') -> None:
        """
        创建表

        Args:
            table_name: 表名
            schema: 列定义
            comment: 表注释
        """
        from odps.models import TableSchema, Column
        from odps.types import odps_schema

        # 构建表结构
        columns = []
        for col in schema:
            # 转换类型
            odps_type = self._convert_mysql_type_to_odps(col['type'])

            column = Column(
                name=col['name'],
                type=odps_type,
                comment=col.get('comment', ''),
                nullable=col.get('is_nullable', True)
            )
            columns.append(column)

        # 创建表
        table_schema = TableSchema(columns=columns)

        self.odps.create_table(
            table_name,
            table_schema,
            comment=comment,
            if_not_exists=True
        )

        logger.info(f"创建表成功: {table_name}")

    def write_dataframe(self, table_name: str,
                       dataframe: pd.DataFrame,
                       mode: str = 'append') -> None:
        """
        写入DataFrame数据

        Args:
            table_name: 表名
            dataframe: 数据
            mode: 写入模式: 'append'或'overwrite'
        """
        # 获取目标表
        table = self.odps.get_table(table_name)

        # 清理数据类型
        cleaned_df = self._clean_dataframe(dataframe, table)

        # 写入数据
        if mode == 'overwrite':
            # 先截断表
            table.truncate()

        # 使用DataFrame API写入
        odps_df = self.odps.write_table(
            table_name,
            cleaned_df.values.tolist(),
            # 使用表的列名
            columns=cleaned_df.columns.tolist()
        )

        logger.info(f"写入数据成功: {len(cleaned_df)} 行")

    def _clean_dataframe(self, df: pd.DataFrame, table) -> pd.DataFrame:
        """清理DataFrame，使其符合MaxCompute要求"""
        import numpy as np
        from datetime import datetime

        cleaned_df = df.copy()

        # 转换datetime类型
        for col in cleaned_df.columns:
            col_type = table.get_column(col).type
            col_type_str = str(col_type).lower()

            # 处理时间戳
            if 'datetime' in col_type_str or 'timestamp' in col_type_str:
                if pd.api.types.is_datetime64_any_dtype(cleaned_df[col]):
                    cleaned_df[col] = cleaned_df[col].dt.tz_localize(None)

            # 处理NULL值
            if not col_type.nullable:
                # 对于非空列，填充默认值
                if 'int' in col_type_str or 'bigint' in col_type_str:
                    cleaned_df[col] = cleaned_df[col].fillna(0)
                elif 'double' in col_type_str or 'float' in col_type_str:
                    cleaned_df[col] = cleaned_df[col].fillna(0.0)
                elif 'string' in col_type_str or 'varchar' in col_type_str:
                    cleaned_df[col] = cleaned_df[col].fillna('')
                elif 'boolean' in col_type_str:
                    cleaned_df[col] = cleaned_df[col].fillna(False)

        return cleaned_df

    def _convert_mysql_type_to_odps(self, mysql_type: str) -> str:
        """转换MySQL类型到MaxCompute类型"""
        type_mapping = {
            'tinyint': 'tinyint',
            'smallint': 'smallint',
            'int': 'int',
            'integer': 'int',
            'bigint': 'bigint',
            'float': 'float',
            'double': 'double',
            'decimal': 'decimal',
            'numeric': 'decimal',
            'varchar': 'string',
            'char': 'string',
            'text': 'string',
            'longtext': 'string',
            'mediumtext': 'string',
            'tinytext': 'string',
            'json': 'string',
            'boolean': 'boolean',
            'bool': 'boolean',
            'date': 'date',
            'datetime': 'datetime',
            'timestamp': 'timestamp',
            'blob': 'binary',
            'longblob': 'binary',
            'mediumblob': 'binary',
            'tinyblob': 'binary',
            'enum': 'string',
            'set': 'string'
        }

        # 提取基础类型（去掉长度等参数）
        base_type = mysql_type.split('(')[0].lower()

        return type_mapping.get(base_type, 'string')
```

### 4.2 修改现有组件

#### 4.2.1 扩展SchemaMapper

**修改文件**: `data_warehouse_migrate/schema_mapper.py`

```python
# 在类中添加新的类方法
class SchemaMapper:
    # ... 现有代码 ...

    # MySQL到MaxCompute的类型映射
    TYPE_MAPPING_MYSQL_TO_MAXCOMPUTE = {
        'tinyint': 'tinyint',
        'smallint': 'smallint',
        'int': 'int',
        'integer': 'int',
        'bigint': 'bigint',
        'float': 'float',
        'double': 'double',
        'decimal': 'decimal',
        'numeric': 'decimal',
        'varchar': 'string',
        'char': 'string',
        'text': 'string',
        'longtext': 'string',
        'json': 'string',
        'boolean': 'boolean',
        'bool': 'boolean',
        'date': 'date',
        'datetime': 'datetime',
        'timestamp': 'timestamp',
        'blob': 'binary',
        'enum': 'string',
        'set': 'string'
    }

    @classmethod
    def convert_mysql_to_maxcompute_schema(cls, mysql_columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将MySQL表结构转换为MaxCompute表结构

        Args:
            mysql_columns: MySQL列信息列表

        Returns:
            MaxCompute列信息列表
        """
        maxcompute_columns = []

        for column in mysql_columns:
            mysql_type = column['type'].lower()

            # 提取基础类型
            base_type = mysql_type.split('(')[0]

            # 转换类型
            maxcompute_type = cls.TYPE_MAPPING_MYSQL_TO_MAXCOMPUTE.get(
                base_type,
                'string'  # 默认转换为string
            )

            # 处理decimal类型的精度
            if base_type in ['decimal', 'numeric']:
                maxcompute_type = mysql_type  # 保留精度信息

            maxcompute_columns.append({
                'name': column['name'],
                'type': maxcompute_type,
                'comment': column.get('comment', ''),
                'is_nullable': column.get('is_nullable', True)
            })

        return maxcompute_columns
```

#### 4.2.2 重构DataMigrator

**修改文件**: `data_warehouse_migrate/migrator.py`

主要修改点：
1. 添加`source_type`参数
2. 将源客户端创建改为动态创建
3. 支持不同的数据流方向

```python
class DataMigrator:
    """数据迁移器（支持双向）"""

    def __init__(self,
                 source_type: str,  # 新增
                 destination_type: str,
                 # 源配置
                 source_project_id: Optional[str] = None,
                 source_table_name: str = None,
                 maxcompute_access_id: Optional[str] = None,
                 maxcompute_secret_key: Optional[str] = None,
                 maxcompute_endpoint: Optional[str] = None,
                 mysql_source_host: Optional[str] = None,
                 mysql_source_user: Optional[str] = None,
                 mysql_source_password: Optional[str] = None,
                 mysql_source_database: Optional[str] = None,
                 mysql_source_port: Optional[int] = None,
                 # 目标配置
                 destination_project_id: Optional[str] = None,
                 destination_table_name: Optional[str] = None,
                 bigquery_credentials_path: Optional[str] = None,
                 mysql_dest_host: Optional[str] = None,
                 mysql_dest_user: Optional[str] = None,
                 mysql_dest_password: Optional[str] = None,
                 mysql_dest_database: Optional[str] = None,
                 mysql_dest_port: Optional[int] = None,
                 # 其他参数保持不变
                 ):
        """
        初始化数据迁移器

        Args:
            source_type: 源数据源类型 (maxcompute 或 mysql)
            destination_type: 目标数据源类型 (bigquery 或 mysql 或 maxcompute)
            ... 其他参数
        """
        self.source_type = source_type
        self.destination_type = destination_type
        self.source_table_name = source_table_name
        self.destination_table_name = destination_table_name

        # 创建源客户端
        self.source_client = self._create_source_client(
            source_type,
            source_project_id=source_project_id,
            maxcompute_access_id=maxcompute_access_id,
            maxcompute_secret_key=maxcompute_secret_key,
            maxcompute_endpoint=maxcompute_endpoint,
            mysql_source_host=mysql_source_host,
            mysql_source_user=mysql_source_user,
            mysql_source_password=mysql_source_password,
            mysql_source_database=mysql_source_database,
            mysql_source_port=mysql_source_port
        )

        # 创建目标客户端
        self.destination_client = self._create_destination_client(
            destination_type,
            destination_project_id=destination_project_id,
            bigquery_credentials_path=bigquery_credentials_path,
            mysql_dest_host=mysql_dest_host,
            mysql_dest_user=mysql_dest_user,
            mysql_dest_password=mysql_dest_password,
            mysql_dest_database=mysql_dest_database,
            mysql_dest_port=mysql_dest_port
        )

        self.schema_mapper = SchemaMapper()

    def _create_source_client(self, source_type: str, **kwargs):
        """创建源客户端"""
        if source_type == 'maxcompute':
            return MaxComputeClient(
                access_id=kwargs.get('maxcompute_access_id'),
                secret_access_key=kwargs.get('maxcompute_secret_key'),
                endpoint=kwargs.get('maxcompute_endpoint'),
                project=kwargs.get('source_project_id')
            )
        elif source_type == 'mysql':
            from .mysql_reader import MySQLReader
            return MySQLReader(
                host=kwargs.get('mysql_source_host'),
                user=kwargs.get('mysql_source_user'),
                password=kwargs.get('mysql_source_password'),
                database=kwargs.get('mysql_source_database'),
                port=kwargs.get('mysql_source_port') or 3306
            )
        else:
            raise ValueError(f"不支持的源类型: {source_type}")

    def _create_destination_client(self, destination_type: str, **kwargs):
        """创建目标客户端（修改现有方法）"""
        if destination_type == 'mysql':
            # 现有代码
            from .mysql_writer import MySQLWriter
            return MySQLWriter(...)
        elif destination_type == 'bigquery':
            # 现有代码
            from .bigquery_client import BigQueryClient
            return BigQueryClient(...)
        elif destination_type == 'maxcompute':
            # 新增
            from .maxcompute_writer import MaxComputeWriter
            return MaxComputeWriter(
                access_id=kwargs.get('maxcompute_access_id'),
                secret_access_key=kwargs.get('maxcompute_secret_key'),
                endpoint=kwargs.get('maxcompute_endpoint'),
                project=kwargs.get('destination_project_id')
            )
        else:
            raise ValueError(f"不支持的目标类型: {destination_type}")

    def _handle_table_schema(self, ...):
        """修改表结构处理逻辑，支持不同的转换方向"""
        if self.source_type == 'maxcompute':
            # 现有逻辑：MaxCompute -> 其他
            if self.destination_type == 'bigquery':
                schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(...)
            elif self.destination_type == 'mysql':
                schema = self.schema_mapper.convert_maxcompute_to_mysql_schema(...)
        elif self.source_type == 'mysql':
            # 新逻辑：MySQL -> MaxCompute
            if self.destination_type == 'maxcompute':
                schema = self.schema_mapper.convert_mysql_to_maxcompute_schema(...)

        # 创建表等逻辑保持不变

    def _migrate_table_data(self, ...):
        """修改数据迁移逻辑，使用对应的客户端方法"""
        # 获取源表数据
        if self.source_type == 'maxcompute':
            data_iter = self.source_client.get_table_data(...)
        elif self.source_type == 'mysql':
            data_iter = self.source_client.get_table_data(...)

        # 写入目标表
        for batch_df in data_iter:
            if self.destination_type == 'maxcompute':
                self.destination_client.write_dataframe(...)
            # ... 其他类型
```

### 4.3 配置扩展

#### 4.3.1 命令行参数扩展

**修改文件**: `data_warehouse_migrate/cli.py`

```python
@click.command()
@click.option('--source-type',
              type=click.Choice(['maxcompute', 'mysql'], case_sensitive=False),
              default=None,
              help='源数据源类型: maxcompute 或 mysql')
# MySQL源配置
@click.option('--mysql-source-host',
              help='MySQL源主机 (当source-type为mysql时需要)')
@click.option('--mysql-source-user',
              help='MySQL源用户名')
@click.option('--mysql-source-password',
              help='MySQL源密码')
@click.option('--mysql-source-database',
              help='MySQL源数据库名')
@click.option('--mysql-source-port',
              type=int,
              help='MySQL源端口')
# MaxCompute目标配置
@click.option('--maxcompute-dest-project-id',
              help='MaxCompute目标项目ID')
@click.option('--maxcompute-dest-access-id',
              help='MaxCompute目标AccessKey ID')
@click.option('--maxcompute-dest-secret-key',
              help='MaxCompute目标AccessKey Secret')
@click.option('--maxcompute-dest-endpoint',
              help='MaxCompute目标Endpoint')
# ... 其他现有参数保持不变

def main(source_type: str,
         mysql_source_host: Optional[str],
         mysql_source_user: Optional[str],
         mysql_source_password: Optional[str],
         mysql_source_database: Optional[str],
         mysql_source_port: Optional[int],
         maxcompute_dest_project_id: Optional[str],
         maxcompute_dest_access_id: Optional[str],
         maxcompute_dest_secret_key: Optional[str],
         maxcompute_dest_endpoint: Optional[str],
         # ... 其他参数
         ):
    """数据仓库迁移工具

    支持以下数据流方向：
    1. MaxCompute -> BigQuery
    2. MaxCompute -> MySQL
    3. MySQL -> MaxCompute
    """

    # 参数验证
    if source_type == 'mysql':
        required_params = ['mysql_source_host', 'mysql_source_user',
                          'mysql_source_password', 'mysql_source_database']
        for param in required_params:
            if not locals()[param]:
                raise click.BadParameter(f"MySQL源配置缺少必需参数: {param}")

    # 创建迁移器
    migrator = DataMigrator(
        source_type=source_type,
        destination_type=destination_type,
        # ... 传递所有参数
    )
```

#### 4.3.2 配置文件示例

**支持的新配置格式**：

```json
{
  "source": {
    "type": "mysql",
    "mysql": {
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "${MYSQL_SOURCE_PASSWORD}",
      "database": "production_db"
    },
    "table_name": "orders"
  },
  "destination": {
    "type": "maxcompute",
    "maxcompute": {
      "project_id": "data_warehouse",
      "access_id": "${MAXCOMPUTE_ACCESS_ID}",
      "secret_key": "${MAXCOMPUTE_SECRET_KEY}",
      "endpoint": "http://service.cn.maxcompute.aliyun.com/api"
    },
    "table_name": "ods_orders"
  },
  "run": {
    "mode": "append",
    "batch_size": 50000,
    "log_level": "INFO"
  },
  "compat": {
    "preserve_string_null_tokens": true,
    "string_null_tokens": ["NULL", "null", ""],
    "null_on_non_nullable": "fill",
    "null_fill_sentinel": ""
  }
}
```

## 5. 高级功能设计

### 5.1 增量同步

```python
class IncrementalSync:
    """增量同步支持"""

    def __init__(self, sync_column: str, last_sync_value: Any):
        """
        Args:
            sync_column: 同步字段名（如id、update_time）
            last_sync_value: 上次同步的最大值
        """
        self.sync_column = sync_column
        self.last_sync_value = last_sync_value

    def get_incremental_sql(self, table_name: str) -> str:
        """生成增量查询SQL"""
        if self.sync_column in ['id', 'created_at', 'updated_at']:
            return f"SELECT * FROM `{table_name}` WHERE `{self.sync_column}` > %s"
        raise ValueError(f"不支持的同步字段: {self.sync_column}")
```

### 5.2 分区表支持

```python
def create_partition_table(self, table_name: str,
                          schema: List[Dict[str, Any]],
                          partition_column: str) -> None:
    """创建分区表"""
    # 识别分区列
    partition_cols = [col for col in schema
                     if col['name'].lower() in ['pt', 'ds', 'partition_date']]

    if partition_cols:
        # 创建分区表SQL
        partition_spec = f"PARTITIONED BY ({partition_col} STRING)"
        # ... 创建分区表逻辑
```

## 6. 实施计划

### Phase 1: 基础功能实现 (2周)
- [ ] 实现MySQLReader类
- [ ] 实现MaxComputeWriter类
- [ ] 扩展SchemaMapper支持MySQL->MaxCompute映射
- [ ] 修改DataMigrator支持新数据流
- [ ] 基础测试

### Phase 2: 功能完善 (1周)
- [ ] 增量同步功能
- [ ] 分区表支持
- [ ] 错误处理优化
- [ ] 性能优化

### Phase 3: 测试和文档 (1周)
- [ ] 单元测试编写
- [ ] 集成测试
- [ ] 文档更新
- [ ] 示例和最佳实践

## 7. 测试用例设计

### 7.1 单元测试

```python
def test_mysql_to_maxcompute_sync():
    """测试MySQL到MaxCompute同步"""
    # 1. 创建测试数据
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
    })

    # 2. 写入MySQL
    # 3. 执行同步
    # 4. 验证MaxCompute中的数据
    assert_data_equal(test_data, maxcompute_data)
```

### 7.2 性能测试

- 测试10万、100万、1000万行数据的同步性能
- 测试不同批次大小的影响
- 测试网络延迟对性能的影响

## 8. 风险与对策

| 风险 | 影响 | 对策 |
|------|------|------|
| 数据类型转换失败 | 同步中断 | 详细的错误日志，支持类型映射配置 |
| 大数据量内存溢出 | 程序崩溃 | 流式处理，合理设置批次大小 |
| 网络中断 | 同步失败 | 断点续传，支持重试机制 |
| 主键冲突 | 数据不一致 | 支持冲突检测和处理策略 |

## 9. 监控指标

- 同步速度（行/秒）
- 错误率
- 内存使用情况
- 网络IO

## 10. 总结

本方案在最小侵入现有代码的前提下，实现了MySQL到MaxCompute的数据同步功能。方案具有以下特点：

1. **架构清晰**：复用现有组件，新增必要的Reader和Writer
2. **功能完整**：支持全量和增量同步，支持分区表
3. **易于扩展**：为未来支持更多数据源方向打下基础
4. **配置灵活**：支持命令行、配置文件等多种配置方式

通过这个方案，用户可以轻松实现MySQL数据到MaxCompute的同步，满足数据仓库的数据补充和回流需求。