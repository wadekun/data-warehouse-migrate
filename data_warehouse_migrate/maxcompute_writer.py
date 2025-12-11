"""
MaxCompute数据写入客户端
"""

from typing import List, Dict, Any, Optional, Union
from odps import ODPS
from odps.models import TableSchema, Table, Column
from odps.df import DataFrame as ODPSDataFrame
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


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
            logger.info(f"成功连接到MaxCompute项目: {self.project}")
        return self._odps

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            # 尝试获取项目空间信息
            self.odps.get_project()
            logger.info("MaxCompute连接测试成功")
            return True
        except Exception as e:
            logger.error(f"MaxCompute连接失败: {e}")
            return False

    def _test_connection(self) -> bool:
        """内部测试连接方法（为了兼容性）"""
        return self.test_connection()

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            self.odps.get_table(table_name)
            return True
        except Exception:
            return False

    def create_table(self, table_name: str,
                    schema: List[Dict[str, Any]],
                    comment: str = '',
                    lifecycle: Optional[int] = None,
                    partition_columns: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        创建表

        Args:
            table_name: 表名
            schema: 列定义
            comment: 表注释
            lifecycle: 生命周期天数
            partition_columns: 分区列定义
        """
        # 构建列
        columns = []
        for col in schema:
            # 转换类型
            odps_type = self._convert_mysql_type_to_odps(col['type'])

            # 处理NULL和默认值
            nullable = col.get('is_nullable', True)

            column = Column(
                name=col['name'],
                type=odps_type,
                comment=col.get('comment', ''),
                nullable=nullable
            )
            columns.append(column)

        # 构建分区列
        partitions = []
        if partition_columns:
            for part_col in partition_columns:
                partitions.append(Column(
                    name=part_col['name'],
                    type=part_col['type'],
                    comment=part_col.get('comment', '')
                ))

        # 创建表结构
        table_schema = TableSchema(columns=columns, partitions=partitions)

        # 创建表
        self.odps.create_table(
            table_name,
            table_schema,
            comment=comment,
            if_not_exists=True,
            lifecycle=lifecycle
        )

        logger.info(f"创建MaxCompute表成功: {table_name}")

    def write_dataframe(self, table_name: str,
                       dataframe: pd.DataFrame,
                       mode: str = 'append',
                       partition_spec: Optional[str] = None) -> None:
        """
        写入DataFrame数据

        Args:
            table_name: 表名
            dataframe: 数据
            mode: 写入模式: 'append'或'overwrite'
            partition_spec: 分区规范，如 'pt=20240101'
        """
        # 获取目标表
        table = self.odps.get_table(table_name)

        # 清理和转换数据
        cleaned_df = self._clean_dataframe(dataframe, table)

        # 处理分区
        if partition_spec and table.table_schema.partitions:
            # 写入分区表
            self._write_partition(table, cleaned_df, partition_spec, mode)
        else:
            # 写入普通表
            self._write_normal_table(table, cleaned_df, mode)

        logger.info(f"写入数据成功: {len(cleaned_df)} 行")

    def _write_partition(self, table: Table, df: pd.DataFrame,
                        partition_spec: str, mode: str):
        """写入分区数据"""
        # 使用tunnel上传数据
        with table.open_writer(partition=partition_spec,
                               create_partition=True,
                               truncate=mode == 'overwrite') as writer:
            # 转换为Record
            for index, row in df.iterrows():
                record = table.new_record()
                for i, col_name in enumerate(df.columns):
                    try:
                        record[i] = self._convert_value_for_odps(
                            row[col_name],
                            table.get_column(col_name).type
                        )
                    except Exception as e:
                        logger.error(f"转换列 {col_name} 的值失败: {e}")
                        record[i] = None
                writer.write(record)

    def _write_normal_table(self, table: Table, df: pd.DataFrame, mode: str):
        """写入普通表数据"""
        # 如果是覆盖模式，先截断表
        if mode == 'overwrite':
            table.truncate()

        # 使用批量写入
        records = []
        batch_size = 1000  # MaxCompute推荐的批量大小

        for index, row in df.iterrows():
            record = table.new_record()
            for i, col_name in enumerate(df.columns):
                try:
                    record[i] = self._convert_value_for_odps(
                        row[col_name],
                        table.get_column(col_name).type
                    )
                except Exception as e:
                    logger.error(f"转换列 {col_name} 的值失败: {e}")
                    record[i] = None
            records.append(record)

            # 批量写入
            if len(records) >= batch_size:
                with table.open_writer(append=True) as writer:
                    writer.write(records)
                records = []

        # 写入剩余记录
        if records:
            with table.open_writer(append=True) as writer:
                writer.write(records)

    def _clean_dataframe(self, df: pd.DataFrame, table: Table) -> pd.DataFrame:
        """清理DataFrame，使其符合MaxCompute要求"""
        cleaned_df = df.copy()

        # 确保列顺序与表结构一致
        table_columns = [col.name for col in table.table_schema.columns]
        if set(cleaned_df.columns) != set(table_columns):
            logger.warning(f"DataFrame列名与表结构不匹配")
            # 重新排序和过滤
            cleaned_df = cleaned_df[table_columns]

        # 转换数据类型
        for col in cleaned_df.columns:
            col_type = table.get_column(col).type
            col_type_str = str(col_type).lower()

            # 处理时间戳
            if 'datetime' in col_type_str:
                if pd.api.types.is_datetime64_any_dtype(cleaned_df[col]):
                    # 移除时区信息，MaxCompute不支持时区
                    if cleaned_df[col].dt.tz is not None:
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
                elif 'date' in col_type_str or 'datetime' in col_type_str:
                    # 日期时间填充当前时间
                    cleaned_df[col] = cleaned_df[col].fillna(datetime.now())

        return cleaned_df

    def _convert_value_for_odps(self, value: Any, target_type) -> Any:
        """转换值为MaxCompute可接受的格式"""
        # 处理None值
        if pd.isna(value) or value is None:
            return None

        # 处理datetime
        if pd.api.types.is_datetime64_any_dtype(value):
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            return value

        # 处理numpy类型
        if isinstance(value, (np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.float64, np.float32)):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)

        # 处理字符串
        if isinstance(value, str):
            # 处理特殊的空值标记
            if value.lower() in ['null', 'none', 'nan']:
                return None
            return value

        return value

    def _convert_mysql_type_to_odps(self, mysql_type: str) -> str:
        """转换MySQL类型到MaxCompute类型"""
        # 提取基础类型（去掉长度等参数）
        base_type = mysql_type.split('(')[0].lower()

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
            'char': 'char',
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

        # 保留decimal的精度信息
        if base_type in ['decimal', 'numeric'] and '(' in mysql_type:
            return mysql_type

        odps_type = type_mapping.get(base_type, 'string')

        # 为char类型添加长度
        if base_type == 'char' and '(' in mysql_type:
            odps_type = mysql_type

        return odps_type

    def truncate_table(self, table_name: str,
                      partition_spec: Optional[str] = None) -> None:
        """清空表数据"""
        table = self.odps.get_table(table_name)

        if partition_spec:
            # 删除特定分区
            table.delete_partition(partition_spec, if_exists=True)
        else:
            # 截断整个表
            table.truncate()

        logger.info(f"清空表数据: {table_name}")

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        table = self.odps.get_table(name=table_name)

        return {
            'name': table.name,
            'size': table.size,
            'comment': table.comment,
            'owner': table.owner,
            'created_time': table.created_time,
            'last_modified': table.last_modified,
            'lifecycle': table.lifecycle,
            'columns': [
                {
                    'name': col.name,
                    'type': str(col.type),
                    'comment': col.comment,
                    'nullable': col.nullable
                }
                for col in table.table_schema.columns
            ],
            'partitions': [
                {
                    'name': part.name,
                    'type': str(part.type),
                    'comment': part.comment
                }
                for part in table.table_schema.partitions or []
            ]
        }