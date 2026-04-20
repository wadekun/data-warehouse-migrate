"""
数据类型映射模块

处理MaxCompute和BigQuery之间的数据类型转换
"""

import re
from typing import Dict, Any, List, Optional, Tuple
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
    def convert_maxcompute_to_mysql_schema(cls, maxcompute_columns: List[Dict[str, Any]], overrides: Dict[str, str] | None = None) -> List[Dict[str, Any]]:
        """
        将MaxCompute表结构转换为MySQL表结构
        
        Args:
            maxcompute_columns: MaxCompute列信息列表
            
        Returns:
            MySQL列信息列表
        """
        mysql_schema: List[Dict[str, Any]] = []
        seen_lower_names: set[str] = set()

        for column in maxcompute_columns:
            # 跳过分区列
            if column.get('is_partition', False):
                logger.info(f"跳过分区字段（MySQL不创建）: {column.get('name')}")
                continue

            name = column['name']
            name_lower = name.lower()
            if name_lower in seen_lower_names:
                logger.warning(f"检测到重复列名 '{name}'，已去重保留首次出现")
                continue

            maxcompute_type = column['type'].lower()

            mysql_type = ""
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

            mysql_schema.append({'name': name, 'type': mysql_type})
            seen_lower_names.add(name_lower)

        # 应用类型覆盖（按目标列名大小写不敏感）
        if overrides and isinstance(overrides, dict):
            lower_override = {str(k).lower(): v for k, v in overrides.items()}
            for col in mysql_schema:
                lname = col['name'].lower()
                if lname in lower_override and lower_override[lname]:
                    col['type'] = lower_override[lname]

        return mysql_schema

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
        seen_lower_names: set[str] = set()

        for column in mysql_columns:
            # 检查重复列名（按小写）
            name = column['name']
            name_lower = name.lower()
            if name_lower in seen_lower_names:
                logger.warning(f"检测到重复列名 '{name}'，已去重保留首次出现")
                continue

            mysql_type = column['type'].lower()

            # 提取基础类型（去掉长度等参数）
            base_type = mysql_type.split('(')[0]

            # 特殊处理ENUM和SET类型
            if base_type in ['enum', 'set']:
                base_type = 'string'
                logger.info(f"列 {name}: 将MySQL {mysql_type} 类型转换为MaxCompute的string类型")

            # 转换类型
            original_type = mysql_type
            maxcompute_type = cls.TYPE_MAPPING_MYSQL_TO_MAXCOMPUTE.get(
                base_type,
                'string'  # 默认转换为string
            )

            # 处理特殊类型
            if base_type in ['decimal', 'numeric'] and '(' in mysql_type:
                # 保留decimal的精度信息
                maxcompute_type = mysql_type
            elif base_type == 'char' and '(' in mysql_type:
                # 保留char的长度信息
                maxcompute_type = mysql_type
            elif base_type == 'tinyint':
                # 检查是否是布尔类型
                if column.get('is_nullable', False):
                    # 可空的tinyint可能是布尔值，保持为tinyint
                    maxcompute_type = 'tinyint'
                else:
                    # 非空的tinyint(1)通常用作布尔值
                    if 'tinyint(1)' in mysql_type:
                        maxcompute_type = 'boolean'
                        logger.warning(f"列 {name}: MySQL {original_type} 被推断为布尔类型，转换为MaxCompute boolean。如果该列用于存储数值，请手动覆盖类型映射")

            # 记录类型转换（如果发生变化）
            if original_type != maxcompute_type and base_type != 'tinyint(1)':
                logger.info(f"列 {name}: MySQL {original_type} -> MaxCompute {maxcompute_type}")

            # 处理MaxCompute不支持的特性
            # MaxCompute不支持默认值（除了NULL）
            # MySQL的AUTO_INCREMENT需要手动处理
            if column.get('default') and column['default'] == 'AUTO_INCREMENT':
                logger.info(f"列 {name} 包含自增属性，MaxCompute不支持，将作为普通整数列")

            maxcompute_columns.append({
                'name': name,
                'type': maxcompute_type,
                'comment': column.get('comment', ''),
                'is_nullable': column.get('is_nullable', True)
            })

            seen_lower_names.add(name_lower)

        logger.info(f"MySQL到MaxCompute结构转换完成，共 {len(maxcompute_columns)} 列")
        return maxcompute_columns

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
    
    # BigQuery NUMERIC 支持的最大 precision/scale
    # NUMERIC: precision<=38, scale<=9 (decimal128, 16 bytes)
    # BIGNUMERIC: precision<=76, scale<=38 (decimal256, 32 bytes)
    _BQ_NUMERIC_MAX_PRECISION = 38
    _BQ_NUMERIC_MAX_SCALE = 9

    @classmethod
    def _parse_decimal_params(cls, maxcompute_type: str) -> Tuple[Optional[int], Optional[int]]:
        """
        解析 decimal(p,s) 的精度与小数位。
        匹配失败返回 (None, None)，由上层按默认规则处理。
        """
        m = re.match(r'\s*decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$', maxcompute_type.lower())
        if m:
            return int(m.group(1)), int(m.group(2))
        return None, None

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

        # decimal 特殊处理：根据 precision/scale 选择 NUMERIC 或 BIGNUMERIC
        # - NUMERIC   : precision<=38 且 scale<=9
        # - BIGNUMERIC: 其余（包含未显式带精度的 decimal，按更宽容的 BIGNUMERIC 处理）
        if base_type == 'decimal':
            precision, scale = cls._parse_decimal_params(maxcompute_type)
            if (precision is not None and scale is not None
                    and precision <= cls._BQ_NUMERIC_MAX_PRECISION
                    and scale <= cls._BQ_NUMERIC_MAX_SCALE):
                return bigquery.enums.SqlTypeNames.NUMERIC
            # 超出 NUMERIC 能力或未知精度，使用 BIGNUMERIC 保留精度
            if precision is not None and scale is not None:
                logger.info(
                    f"decimal({precision},{scale}) 超出 BigQuery NUMERIC 能力(p<=38,s<=9)，"
                    f"使用 BIGNUMERIC"
                )
            return bigquery.enums.SqlTypeNames.BIGNUMERIC

        if base_type in cls.TYPE_MAPPING:
            return cls.TYPE_MAPPING[base_type]
        else:
            logger.warning(f"未知的MaxCompute类型: {maxcompute_type}，使用STRING类型")
            return bigquery.enums.SqlTypeNames.STRING
