"""
数据类型映射模块

处理MaxCompute和BigQuery之间的数据类型转换
"""

from typing import Dict, Any, List
from google.cloud import bigquery

from .exceptions import SchemaConversionError
from .logger import setup_logger

logger = setup_logger(__name__)


class SchemaMapper:
    """数据类型映射器"""
    
    # MaxCompute到BigQuery的数据类型映射
    TYPE_MAPPING = {
        'bigint': bigquery.enums.SqlTypeNames.INT64,
        'int': bigquery.enums.SqlTypeNames.INT64,
        'smallint': bigquery.enums.SqlTypeNames.INT64,
        'tinyint': bigquery.enums.SqlTypeNames.INT64,
        'double': bigquery.enums.SqlTypeNames.FLOAT64,
        'float': bigquery.enums.SqlTypeNames.FLOAT64,
        'decimal': bigquery.enums.SqlTypeNames.NUMERIC,
        'string': bigquery.enums.SqlTypeNames.STRING,
        'varchar': bigquery.enums.SqlTypeNames.STRING,
        'char': bigquery.enums.SqlTypeNames.STRING,
        'boolean': bigquery.enums.SqlTypeNames.BOOL,
        'datetime': bigquery.enums.SqlTypeNames.DATETIME,
        'timestamp': bigquery.enums.SqlTypeNames.TIMESTAMP,
        'date': bigquery.enums.SqlTypeNames.DATE,
        'binary': bigquery.enums.SqlTypeNames.BYTES,
        'map': bigquery.enums.SqlTypeNames.RECORD,
        'struct': bigquery.enums.SqlTypeNames.RECORD,
    }
    
    @classmethod
    def convert_maxcompute_to_bigquery_schema(cls, maxcompute_columns: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """
        将MaxCompute表结构转换为BigQuery表结构
        
        Args:
            maxcompute_columns: MaxCompute列信息列表
            
        Returns:
            BigQuery SchemaField列表
        """
        bigquery_fields = []
        
        for column in maxcompute_columns:
            try:
                # 跳过分区字段，BigQuery不需要显式的分区字段定义
                if column.get('is_partition', False):
                    logger.info(f"跳过分区字段: {column['name']}")
                    continue

                field = cls._convert_column(column)
                bigquery_fields.append(field)
            except Exception as e:
                logger.error(f"转换列 {column.get('name', 'unknown')} 时出错: {e}")
                raise SchemaConversionError(f"无法转换列 {column.get('name', 'unknown')}: {e}")
        
        return bigquery_fields
    
    @classmethod
    def _convert_column(cls, column: Dict[str, Any]) -> bigquery.SchemaField:
        """
        转换单个列定义
        
        Args:
            column: MaxCompute列信息
            
        Returns:
            BigQuery SchemaField
        """
        name = column['name']
        maxcompute_type = column['type'].lower()
        comment = column.get('comment', '')
        
        # 处理复杂类型
        if maxcompute_type.startswith('array<'):
            # 数组类型
            inner_type = maxcompute_type[6:-1]  # 去掉 array< 和 >
            inner_field_type = cls._get_bigquery_type(inner_type)
            return bigquery.SchemaField(
                name=name,
                field_type=inner_field_type,
                mode='REPEATED',
                description=comment
            )
        elif maxcompute_type.startswith('map<'):
            # Map类型转换为RECORD
            logger.warning(f"Map类型 {maxcompute_type} 将转换为RECORD类型，可能需要手动调整")
            return bigquery.SchemaField(
                name=name,
                field_type=bigquery.enums.SqlTypeNames.RECORD,
                mode='NULLABLE',
                description=comment,
                fields=[
                    bigquery.SchemaField('key', bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField('value', bigquery.enums.SqlTypeNames.STRING)
                ]
            )
        elif maxcompute_type.startswith('struct<'):
            # Struct类型
            logger.warning(f"Struct类型 {maxcompute_type} 需要手动解析字段结构")
            return bigquery.SchemaField(
                name=name,
                field_type=bigquery.enums.SqlTypeNames.RECORD,
                mode='NULLABLE',
                description=comment
            )
        else:
            # 基础类型
            field_type = cls._get_bigquery_type(maxcompute_type)
            return bigquery.SchemaField(
                name=name,
                field_type=field_type,
                mode='NULLABLE',
                description=comment
            )
    
    @classmethod
    def _get_bigquery_type(cls, maxcompute_type: str) -> str:
        """
        获取对应的BigQuery数据类型
        
        Args:
            maxcompute_type: MaxCompute数据类型
            
        Returns:
            BigQuery数据类型
        """
        # 处理带精度的类型，如 decimal(10,2)
        base_type = maxcompute_type.split('(')[0].lower()
        
        if base_type in cls.TYPE_MAPPING:
            return cls.TYPE_MAPPING[base_type]
        else:
            logger.warning(f"未知的MaxCompute类型: {maxcompute_type}，使用STRING类型")
            return bigquery.enums.SqlTypeNames.STRING
