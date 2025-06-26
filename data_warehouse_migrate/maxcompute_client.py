"""
MaxCompute客户端模块
"""

from typing import List, Dict, Any, Optional, Iterator
import pandas as pd
from odps import ODPS
from odps.models import Table

from .config import config
from .exceptions import MaxComputeConnectionError, TableNotFoundError
from .logger import setup_logger

logger = setup_logger(__name__)


class MaxComputeClient:
    """MaxCompute客户端"""
    
    def __init__(self, access_id: Optional[str] = None, 
                 secret_access_key: Optional[str] = None,
                 endpoint: Optional[str] = None,
                 project: Optional[str] = None):
        """
        初始化MaxCompute客户端
        
        Args:
            access_id: AccessKey ID
            secret_access_key: AccessKey Secret
            endpoint: MaxCompute endpoint
            project: 项目名称
        """
        self.access_id = access_id or config.maxcompute_access_id
        self.secret_access_key = secret_access_key or config.maxcompute_secret_access_key
        self.endpoint = endpoint or config.maxcompute_endpoint
        self.project = project
        
        if not all([self.access_id, self.secret_access_key, self.endpoint]):
            raise MaxComputeConnectionError("MaxCompute连接参数不完整")
        
        self._odps = None
    
    @property
    def odps(self) -> ODPS:
        """获取ODPS实例"""
        if self._odps is None:
            try:
                self._odps = ODPS(
                    access_id=self.access_id,
                    secret_access_key=self.secret_access_key,
                    project=self.project,
                    endpoint=self.endpoint
                )
                logger.info(f"成功连接到MaxCompute项目: {self.project}")
            except Exception as e:
                logger.error(f"连接MaxCompute失败: {e}")
                raise MaxComputeConnectionError(f"连接MaxCompute失败: {e}")
        
        return self._odps
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            表结构信息列表
        """
        try:
            table = self.odps.get_table(table_name)
            if table is None:
                raise TableNotFoundError(f"表 {table_name} 不存在")

            columns = []

            # 添加普通列
            for column in table.table_schema.columns:
                columns.append({
                    'name': column.name,
                    'type': str(column.type),
                    'comment': column.comment or '',
                    'is_partition': False
                })

            # 添加分区列
            if hasattr(table.table_schema, 'partitions') and table.table_schema.partitions:
                for partition in table.table_schema.partitions:
                    columns.append({
                        'name': partition.name,
                        'type': str(partition.type),
                        'comment': partition.comment or '',
                        'is_partition': True
                    })
                logger.info(f"表 {table_name} 包含 {len(table.table_schema.partitions)} 个分区字段")

            logger.info(f"成功获取表 {table_name} 的结构信息，共 {len(columns)} 列")
            return columns

        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            raise TableNotFoundError(f"获取表 {table_name} 结构失败: {e}")
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None,
                      batch_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        获取表数据

        Args:
            table_name: 表名
            limit: 限制行数
            batch_size: 批次大小

        Yields:
            DataFrame批次数据
        """
        try:
            table = self.odps.get_table(table_name)
            if table is None:
                raise TableNotFoundError(f"表 {table_name} 不存在")

            # 构建查询SQL，处理分区表
            sql = self._build_select_sql(table, table_name, limit)

            logger.info(f"开始读取表 {table_name} 的数据，SQL: {sql}")
            
            # 执行查询并分批返回数据
            with self.odps.execute_sql(sql).open_reader() as reader:
                batch_data = []
                row_count = 0
                
                for record in reader:
                    # 将记录转换为字典，只进行基础清理，不进行类型转换
                    row_dict = {}
                    for i, value in enumerate(record.values):
                        column_name = reader.schema.columns[i].name

                        # 只进行基础的值清理，保持原始类型
                        cleaned_value = self._basic_clean_value(value)
                        row_dict[column_name] = cleaned_value

                    batch_data.append(row_dict)
                    row_count += 1
                    
                    # 当达到批次大小时，返回DataFrame
                    if len(batch_data) >= batch_size:
                        df = pd.DataFrame(batch_data)
                        logger.debug(f"返回批次数据，行数: {len(df)}")
                        yield df
                        batch_data = []
                
                # 返回最后一批数据
                if batch_data:
                    df = pd.DataFrame(batch_data)
                    logger.debug(f"返回最后批次数据，行数: {len(df)}")
                    yield df
                
                logger.info(f"完成读取表 {table_name}，总行数: {row_count}")
                
        except Exception as e:
            logger.error(f"读取表数据失败: {e}")
            raise MaxComputeConnectionError(f"读取表 {table_name} 数据失败: {e}")

    def _build_select_sql(self, table, table_name: str, limit: Optional[int] = None) -> str:
        """
        构建查询SQL，处理分区表

        Args:
            table: MaxCompute表对象
            table_name: 表名
            limit: 限制行数

        Returns:
            构建的SQL语句
        """
        sql = f"SELECT * FROM {table_name}"

        # 检查是否为分区表
        if hasattr(table.table_schema, 'partitions') and table.table_schema.partitions:
            partition_columns = [p.name for p in table.table_schema.partitions]
            logger.info(f"表 {table_name} 是分区表，分区字段: {partition_columns}")

            # 检查是否有pt分区字段
            if 'pt' in partition_columns:
                latest_partition = self._get_latest_partition(table_name, 'pt')
                if latest_partition:
                    sql += f" WHERE pt = '{latest_partition}'"
                    logger.info(f"使用最新分区: pt = '{latest_partition}'")
                else:
                    logger.warning(f"表 {table_name} 没有找到pt分区数据，将尝试查询所有分区")
                    # 如果pt分区没有数据，尝试不使用分区条件，但添加LIMIT来避免全表扫描
                    if not limit:
                        sql += " LIMIT 100000"  # 默认限制10万行
                        logger.warning("为避免全表扫描，自动添加LIMIT 100000")
            else:
                # 如果有其他分区字段，获取最新分区
                latest_partitions = self._get_latest_partitions(table_name)
                if latest_partitions:
                    conditions = []
                    for col, value in latest_partitions.items():
                        conditions.append(f"{col} = '{value}'")
                    sql += f" WHERE {' AND '.join(conditions)}"
                    logger.info(f"使用最新分区: {latest_partitions}")
                else:
                    logger.warning(f"表 {table_name} 没有找到分区数据，将尝试查询所有分区")
                    # 如果没有找到分区数据，添加LIMIT来避免全表扫描
                    if not limit:
                        sql += " LIMIT 100000"  # 默认限制10万行
                        logger.warning("为避免全表扫描，自动添加LIMIT 100000")
        else:
            logger.info(f"表 {table_name} 不是分区表")

        if limit:
            sql += f" LIMIT {limit}"

        return sql

    def _get_latest_partition(self, table_name: str, partition_column: str) -> Optional[str]:
        """
        获取指定分区字段的最新分区值

        Args:
            table_name: 表名
            partition_column: 分区字段名

        Returns:
            最新分区值
        """
        try:
            # 先检查分区是否有数据
            check_sql = f"SELECT COUNT(1) FROM {table_name} WHERE {partition_column} IS NOT NULL LIMIT 1"

            with self.odps.execute_sql(check_sql).open_reader() as reader:
                for record in reader:
                    if record[0] == 0:
                        logger.warning(f"表 {table_name} 的分区字段 {partition_column} 没有数据")
                        return None

            # 查询最新分区
            sql = f"""
            SELECT MAX({partition_column}) as latest_partition
            FROM {table_name}
            WHERE {partition_column} IS NOT NULL
            """

            with self.odps.execute_sql(sql).open_reader() as reader:
                for record in reader:
                    latest_partition = record[0]
                    if latest_partition:
                        return str(latest_partition)

            return None

        except Exception as e:
            logger.error(f"获取最新分区失败: {e}")
            # 如果查询分区失败，尝试不使用分区条件
            logger.warning(f"将尝试不使用分区条件查询表 {table_name}")
            return None

    def _get_latest_partitions(self, table_name: str) -> Dict[str, str]:
        """
        获取所有分区字段的最新分区值

        Args:
            table_name: 表名

        Returns:
            分区字段和值的字典
        """
        try:
            table = self.odps.get_table(table_name)
            if not hasattr(table.table_schema, 'partitions') or not table.table_schema.partitions:
                return {}

            partition_columns = [p.name for p in table.table_schema.partitions]

            # 构建查询最新分区的SQL
            select_parts = []
            for col in partition_columns:
                select_parts.append(f"MAX({col}) as latest_{col}")

            sql = f"""
            SELECT {', '.join(select_parts)}
            FROM {table_name}
            WHERE {' AND '.join([f'{col} IS NOT NULL' for col in partition_columns])}
            """

            latest_partitions = {}
            with self.odps.execute_sql(sql).open_reader() as reader:
                for record in reader:
                    for i, col in enumerate(partition_columns):
                        value = record[i]
                        if value:
                            latest_partitions[col] = str(value)

            return latest_partitions

        except Exception as e:
            logger.error(f"获取最新分区失败: {e}")
            return {}

    def validate_table_access(self, table_name: str) -> bool:
        """
        验证表是否可以正常访问（特别是分区表）

        Args:
            table_name: 表名

        Returns:
            是否可以访问
        """
        try:
            table = self.odps.get_table(table_name)
            if table is None:
                return False

            # 构建测试查询SQL
            test_sql = self._build_select_sql(table, table_name, limit=1)

            # 执行测试查询
            with self.odps.execute_sql(test_sql).open_reader() as reader:
                # 只读取第一行来验证查询是否可行
                for _ in reader:
                    break

            logger.info(f"表 {table_name} 访问验证成功")
            return True

        except Exception as e:
            logger.error(f"表 {table_name} 访问验证失败: {e}")
            return False

    def test_connection(self) -> bool:
        """
        测试连接

        Returns:
            连接是否成功
        """
        try:
            # 尝试列出项目中的表（使用正确的参数名）
            tables = list(self.odps.list_tables())
            if len(tables) > 0:
                logger.info(f"MaxCompute连接测试成功，项目中有 {len(tables)} 个表")
            else:
                logger.info("MaxCompute连接测试成功，项目中暂无表")
            return True
        except Exception as e:
            logger.error(f"MaxCompute连接测试失败: {e}")
            return False

    def _basic_clean_value(self, value: Any) -> Any:
        """
        基础的值清理，不进行类型转换

        Args:
            value: 原始值

        Returns:
            清理后的值
        """
        if value is None:
            return None

        # 对于字符串类型，只清理明显的NULL表示
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.lower() in ['null', 'none', '']:
                return None
            return value  # 保持原始字符串值

        # 其他类型直接返回
        return value

    def _clean_value(self, value: Any, column_type: str, column_name: str) -> Any:
        """
        清理和转换数据值，确保与BigQuery兼容

        Args:
            value: 原始值
            column_type: 列类型
            column_name: 列名

        Returns:
            清理后的值
        """
        if value is None:
            return None

        try:
            # 处理整数类型
            if any(t in column_type for t in ['bigint', 'int', 'smallint', 'tinyint']):
                if isinstance(value, str):
                    # 处理空字符串
                    if value.strip() == '' or value.strip().lower() in ['null', 'none']:
                        return None
                    # 处理数字字符串
                    try:
                        # 先转float再转int，处理"1.0"这样的情况
                        float_val = float(value)
                        if float_val != float_val:  # 检查NaN
                            return None
                        return int(float_val)
                    except ValueError:
                        logger.warning(f"列 {column_name} 的值 '{value}' 无法转换为整数，设为None")
                        return None
                elif isinstance(value, (int, float)):
                    if value != value:  # 检查NaN
                        return None
                    return int(value)
                else:
                    return value

            # 处理浮点数类型
            elif any(t in column_type for t in ['double', 'float', 'decimal']):
                if isinstance(value, str):
                    if value.strip() == '' or value.strip().lower() in ['null', 'none']:
                        return None
                    try:
                        return float(value)
                    except ValueError:
                        logger.warning(f"列 {column_name} 的值 '{value}' 无法转换为浮点数，设为None")
                        return None
                elif isinstance(value, (int, float)):
                    return float(value)
                else:
                    return value

            # 处理布尔类型
            elif 'boolean' in column_type:
                if isinstance(value, str):
                    value_lower = value.strip().lower()
                    if value_lower in ['true', '1', 'yes', 'y']:
                        return True
                    elif value_lower in ['false', '0', 'no', 'n', '']:
                        return False
                    else:
                        logger.warning(f"列 {column_name} 的值 '{value}' 无法转换为布尔值，设为False")
                        return False
                elif isinstance(value, (int, float)):
                    return bool(value)
                else:
                    return bool(value)

            # 处理字符串类型
            elif any(t in column_type for t in ['string', 'varchar', 'char']):
                if value is None:
                    return None
                return str(value)

            # 处理日期时间类型
            elif any(t in column_type for t in ['datetime', 'timestamp', 'date']):
                if isinstance(value, str):
                    if value.strip() == '' or value.strip().lower() in ['null', 'none']:
                        return None
                return value

            # 其他类型直接返回
            else:
                return value

        except Exception as e:
            logger.warning(f"清理列 {column_name} (类型: {column_type}) 的值 '{value}' 时出错: {e}，设为None")
            return None
