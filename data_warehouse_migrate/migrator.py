"""
数据迁移核心模块
"""

from typing import Optional, List, Dict, Any
import string
from enum import Enum
from tqdm import tqdm
import pandas as pd
import numpy as np
from google.cloud import bigquery

from .maxcompute_client import MaxComputeClient
from .bigquery_client import BigQueryClient
from .mysql_writer import MySQLWriter
from .schema_mapper import SchemaMapper
from .exceptions import DataMigrationError, ConfigurationError
from .logger import setup_logger

logger = setup_logger(__name__)


class MigrationMode(Enum):
    """迁移模式"""
    OVERWRITE = "overwrite"
    APPEND = "append"


class DataMigrator:
    """数据迁移器"""
    
    def __init__(self, 
                 source_project_id: str,
                 destination_type: str,
                 destination_project_id: Optional[str] = None,
                 maxcompute_access_id: Optional[str] = None,
                 maxcompute_secret_key: Optional[str] = None,
                 maxcompute_endpoint: Optional[str] = None,
                 bigquery_credentials_path: Optional[str] = None,
                 mysql_dest_host: Optional[str] = None,
                 mysql_dest_user: Optional[str] = None,
                 mysql_dest_password: Optional[str] = None,
                 mysql_dest_database: Optional[str] = None,
                 mysql_dest_port: Optional[int] = None,
                 preserve_string_null_tokens: bool = True,
                 string_null_tokens: Optional[List[str]] = None,
                 null_on_non_nullable: str = 'fail',
                 null_fill_sentinel: Optional[str] = None,
                 column_mapping_plan: Optional[Dict[str, Any]] = None):
        """
        初始化数据迁移器
        
        Args:
            source_project_id: MaxCompute项目ID
            destination_type: 目标数据源类型 (bigquery 或 mysql)
            destination_project_id: BigQuery项目ID (仅当destination_type为bigquery时需要)
            maxcompute_access_id: MaxCompute AccessKey ID
            maxcompute_secret_key: MaxCompute AccessKey Secret
            maxcompute_endpoint: MaxCompute endpoint
            bigquery_credentials_path: BigQuery凭证文件路径
            mysql_dest_host: MySQL目标主机
            mysql_dest_user: MySQL目标用户名
            mysql_dest_password: MySQL目标密码
            mysql_dest_database: MySQL目标数据库
            mysql_dest_port: MySQL目标端口
        """
        self.source_project_id = source_project_id
        self.destination_type = destination_type
        
        # 初始化MaxCompute客户端
        self.maxcompute_client = MaxComputeClient(
            access_id=maxcompute_access_id,
            secret_access_key=maxcompute_secret_key,
            endpoint=maxcompute_endpoint,
            project=source_project_id
        )
        
        # 初始化目标客户端
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
        self._source_schema_cache = {}  # 缓存源表结构
        self._destination_schema_cache = {}  # 缓存目标表结构

        # 字符串空值与非空策略
        self.preserve_string_null_tokens = preserve_string_null_tokens
        self.string_null_tokens = string_null_tokens or ['nan', 'None', 'null', '<NA>', 'NaN']
        self.null_on_non_nullable = (null_on_non_nullable or 'fail').lower()
        self.null_fill_sentinel = null_fill_sentinel
        self.column_mapping_plan = column_mapping_plan if column_mapping_plan else None

    def _create_destination_client(self, destination_type: str, **kwargs):
        if destination_type == 'mysql':
            if not all([kwargs.get('mysql_dest_host'), kwargs.get('mysql_dest_user'), kwargs.get('mysql_dest_password'), kwargs.get('mysql_dest_database')]):
                raise ConfigurationError("MySQL目标配置不完整")
            return MySQLWriter(
                host=kwargs.get('mysql_dest_host'),
                user=kwargs.get('mysql_dest_user'),
                password=kwargs.get('mysql_dest_password'),
                database=kwargs.get('mysql_dest_database'),
                port=(kwargs.get('mysql_dest_port') or 3306)
            )
        elif destination_type == 'bigquery':
            if not all([kwargs.get('bigquery_credentials_path'), kwargs.get('destination_project_id')]):
                raise ConfigurationError("BigQuery凭证路径或项目ID未提供")
            return BigQueryClient(
                project_id=kwargs.get('destination_project_id'),
                credentials_path=kwargs.get('bigquery_credentials_path')
            )
        else:
            raise ValueError(f"不支持的目标类型: {destination_type}")
    
    def migrate_table(self, 
                     source_table_name: str,
                     destination_table_name: str,
                     mode: MigrationMode = MigrationMode.APPEND,
                     batch_size: int = 10000,
                     destination_dataset_id: Optional[str] = None,
                     destination_database: Optional[str] = None) -> None:
        """
        迁移表数据
        
        Args:
            source_table_name: 源表名
            destination_table_name: 目标表名
            mode: 迁移模式
            batch_size: 批次大小
            destination_dataset_id: BigQuery目标数据集ID (仅当destination_type为bigquery时需要)
            destination_database: MySQL目标数据库名 (仅当destination_type为mysql时需要)
        """
        try:
            logger.info(f"开始迁移表: {source_table_name} -> {destination_table_name}")
            
            # 1. 测试连接
            self._test_connections()
            
            # 2. 处理表结构
            self._handle_table_schema(
                source_table_name, 
                destination_table_name, 
                mode,
                destination_dataset_id,
                destination_database
            )
            
            # 3. 迁移数据
            self._migrate_table_data(
                source_table_name,
                destination_table_name,
                mode,
                batch_size
            )
            
            logger.info(f"表迁移完成: {source_table_name} -> {destination_table_name}")
            
        except Exception as e:
            logger.error(f"表迁移失败: {e}")
            raise DataMigrationError(f"表迁移失败: {e}")
    
    def _test_connections(self) -> None:
        """测试连接"""
        logger.info("测试数据库连接...")
        
        if not self.maxcompute_client.test_connection():
            raise DataMigrationError("MaxCompute连接失败")
        
        if not self.destination_client._test_connection():
            raise DataMigrationError(f"目标数据库 ({self.destination_type}) 连接失败")
        
        logger.info("数据库连接测试通过")
    
    def _handle_table_schema(self, 
                           source_table_name: str,
                           destination_table_name: str,
                           mode: MigrationMode,
                           destination_dataset_id: Optional[str] = None,
                           destination_database: Optional[str] = None) -> None:
        """处理表结构"""
        logger.info("处理表结构...")
        
        # 检查目标表是否存在
        if self.destination_type == 'bigquery':
            if not destination_dataset_id:
                raise ConfigurationError("BigQuery目标数据集ID未提供")
            # BigQuery需要先创建数据集
            self.destination_client.create_dataset_if_not_exists(destination_dataset_id)
            table_exists = self.destination_client.table_exists(
                destination_dataset_id,
                destination_table_name
            )
        elif self.destination_type == 'mysql':
            if not destination_database:
                raise ConfigurationError("MySQL目标数据库名未提供")
            table_exists = self.destination_client.table_exists(
                destination_database,
                destination_table_name
            )
        else:
            raise ValueError(f"不支持的目标类型: {self.destination_type}")
        
        if table_exists:
            if mode == MigrationMode.OVERWRITE:
                if self.destination_type == 'mysql':
                    logger.info("目标MySQL表已存在，执行TRUNCATE TABLE清空数据")
                    self.destination_client.truncate_table(destination_table_name)
                else: # BigQuery overwrite logic (drop and recreate)
                    logger.info("目标BigQuery表已存在，删除并重新创建")
                    self.destination_client.delete_table(
                        destination_dataset_id,
                        destination_table_name
                    )
                    table_exists = False # Recreate table
            else: # mode == MigrationMode.APPEND
                logger.info("目标表已存在，将追加数据")
        
        if not table_exists:
            # 验证源表访问权限（特别是分区表）
            logger.info(f"验证源表 {source_table_name} 的访问权限")
            if not self.maxcompute_client.validate_table_access(source_table_name):
                raise DataMigrationError(f"无法访问源表 {source_table_name}，请检查表是否存在或分区配置")

            # 获取源表结构
            logger.info(f"获取源表 {source_table_name} 的结构")
            maxcompute_columns = self.maxcompute_client.get_table_schema(source_table_name)

            # 转换为目标结构
            logger.info("转换表结构")
            if self.destination_type == 'bigquery':
                destination_schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(
                    maxcompute_columns
                )
            elif self.destination_type == 'mysql':
                # 应用字段映射计划（仅 MySQL 一期）
                if self.column_mapping_plan:
                    self._validate_mapping_mysql(self.column_mapping_plan, maxcompute_columns)
                    prepared_columns, overrides, _ = self._prepare_mysql_schema_inputs(maxcompute_columns, self.column_mapping_plan)
                    destination_schema = self.schema_mapper.convert_maxcompute_to_mysql_schema(
                        prepared_columns,
                        overrides=overrides
                    )
                else:
                    destination_schema = self.schema_mapper.convert_maxcompute_to_mysql_schema(
                        maxcompute_columns
                    )
                # 兜底去重，避免重复列（大小写不敏感）
                destination_schema = self._dedupe_mysql_schema(destination_schema)
            else:
                raise ValueError(f"不支持的目标类型: {self.destination_type}")

            # 创建目标表
            logger.info(f"创建目标表 {destination_table_name}")
            if self.destination_type == 'bigquery':
                self.destination_client.create_table(
                    destination_dataset_id,
                    destination_table_name,
                    destination_schema,
                    f"从MaxCompute表 {source_table_name} 迁移"
                )
            elif self.destination_type == 'mysql':
                self.destination_client.create_table(
                    destination_table_name,
                    destination_schema,
                    mode.value
                )
            else:
                raise ValueError(f"不支持的目标类型: {self.destination_type}")
    
    def _migrate_table_data(self, 
                          source_table_name: str,
                          destination_table_name: str,
                          mode: MigrationMode,
                          batch_size: int) -> None:
        """迁移表数据"""
        logger.info("开始迁移数据...")

        # Get source schema (already available)
        maxcompute_columns = self.maxcompute_client.get_table_schema(source_table_name)

        batch_count = 0
        total_rows = 0

        try:
            data_iterator = self.maxcompute_client.get_table_data(
                source_table_name, 
                batch_size=batch_size
            )
            
            for batch_df in tqdm(data_iterator, desc="迁移数据批次"):
                batch_count += 1
                batch_rows = len(batch_df)
                total_rows += batch_rows
                
                logger.debug(f"处理第 {batch_count} 批数据，行数: {batch_rows}")

                # 先按源表结构进行类型应用（不裁剪列，避免映射前丢列）
                typed_df = self._apply_source_schema_types(batch_df, source_table_name)

                # Apply MySQL defaults for non-nullable columns if destination is MySQL
                if self.destination_type == 'mysql':
                    # 应用映射的数据变换（选择/重命名/计算/默认/排序）
                    if self.column_mapping_plan:
                        typed_df = self._transform_dataframe_by_mapping_mysql(typed_df, self.column_mapping_plan)
                    # Get destination schema (full info including nullability and defaults)
                    # Cache this to avoid repeated calls
                    if destination_table_name not in self._destination_schema_cache:
                        full_destination_schema = self.destination_client.get_table_schema(destination_table_name)
                        self._destination_schema_cache[destination_table_name] = full_destination_schema
                    else:
                        full_destination_schema = self._destination_schema_cache[destination_table_name]

                    # 依据目标表列集合过滤并按表顺序重排
                    dest_cols_order = [c['name'] for c in full_destination_schema]
                    dest_cols_lower = {c.lower() for c in dest_cols_order}
                    keep_cols = [c for c in typed_df.columns if c.lower() in dest_cols_lower]
                    typed_df = typed_df[keep_cols]
                    typed_df = typed_df[[c for c in dest_cols_order if c in typed_df.columns]]

                    # 先应用默认值（若列非空且存在默认值）
                    typed_df = self._apply_mysql_defaults(typed_df, full_destination_schema)
                    # 再进行非空列校验/处理
                    typed_df = self._validate_non_nullable_columns_before_write(typed_df, full_destination_schema)

                # 加载数据到目标
                self.destination_client.write_dataframe(
                    destination_table_name,
                    typed_df,
                    mode.value # Pass mode as string (overwrite/append)
                )
            
            logger.info(f"数据迁移完成，总共处理 {batch_count} 批次，{total_rows} 行数据")
            
        except Exception as e:
            logger.error(f"数据迁移过程中出错: {e}")
            raise DataMigrationError(f"数据迁移失败: {e}")

    def _apply_source_schema_types(self, df, source_table_name: str):
        """
        根据源表结构应用正确的数据类型，而不是进行类型推断

        Args:
            df: 原始DataFrame
            source_table_name: 源表名

        Returns:
            应用了正确类型的DataFrame
        """
        try:
            # 获取或使用缓存的源表结构
            if source_table_name not in self._source_schema_cache:
                source_columns = self.maxcompute_client.get_table_schema(source_table_name)

                # 创建列名到类型的映射
                column_types = {}
                for col_info in source_columns:
                    if not col_info.get('is_partition', False):  # 跳过分区字段
                        column_types[col_info['name']] = col_info['type'].lower()

                self._source_schema_cache[source_table_name] = column_types
                logger.info(f"缓存源表 {source_table_name} 的结构信息，共 {len(column_types)} 列")
            else:
                column_types = self._source_schema_cache[source_table_name]
                logger.debug(f"使用缓存的源表 {source_table_name} 结构信息")

            typed_df = df.copy()

            for column in typed_df.columns:
                if column in column_types:
                    maxcompute_type = column_types[column]
                    try:
                        typed_df[column] = self._convert_column_by_source_type(
                            typed_df[column], column, maxcompute_type
                        )
                        # 修复：强制string/varchar/char类型字段为object类型，防止float64残留
                        if any(t in maxcompute_type for t in ['string', 'varchar', 'char']):
                            typed_df[column] = typed_df[column].astype(object)
                    except Exception as e:
                        logger.warning(f"根据源表类型转换列 {column} 失败: {e}，保持原始类型")
                else:
                    logger.warning(f"列 {column} 在源表结构中未找到，保持原始类型")

            return typed_df

        except Exception as e:
            logger.error(f"应用源表结构类型失败: {e}，回退到基础清理")
            return self._basic_dataframe_cleanup(df)

    def _convert_column_by_source_type(self, series: pd.Series, column_name: str, maxcompute_type: str) -> pd.Series:
        """
        根据MaxCompute源表类型转换列数据

        Args:
            series: pandas Series
            column_name: 列名
            maxcompute_type: MaxCompute数据类型

        Returns:
            转换后的Series
        """
        logger.debug(f"列 {column_name} 源表类型: {maxcompute_type}")

        # 处理整数类型
        if any(t in maxcompute_type for t in ['bigint', 'int', 'smallint', 'tinyint']):
            # 源表定义为整数类型，尝试转换
            converted = pd.to_numeric(series, errors='coerce')
            if converted.isna().any():
                # 有无法转换的值，使用float64以保持NaN
                result = converted.astype('float64')
                logger.debug(f"列 {column_name} 转换为float64（源表为整数类型但包含无法转换的值）")
            else:
                # 所有值都可以转换，使用int64
                result = converted.astype('int64')
                logger.debug(f"列 {column_name} 转换为int64（源表为整数类型）")
            return result

        # 处理浮点数类型
        elif any(t in maxcompute_type for t in ['double', 'float', 'decimal']):
            result = pd.to_numeric(series, errors='coerce').astype('float64')
            logger.debug(f"列 {column_name} 转换为float64（源表为浮点类型）")
            return result

        # 处理布尔类型
        elif 'boolean' in maxcompute_type:
            # 转换布尔值
            def convert_bool(val):
                if pd.isna(val) or val is None:
                    return None
                val_str = str(val).strip().lower()
                if val_str in ['true', '1', 'yes', 'y']:
                    return True
                elif val_str in ['false', '0', 'no', 'n', '']:
                    return False
                else:
                    return None

            result = series.apply(convert_bool)
            logger.debug(f"列 {column_name} 转换为布尔类型（源表为boolean类型）")
            return result

        # 处理字符串类型
        elif any(t in maxcompute_type for t in ['string', 'varchar', 'char']):
            # 源表定义为字符串类型，强制保持字符串
            logger.debug(f"列 {column_name} 源表定义为字符串类型 ({maxcompute_type})，强制保持字符串")

            # 强制转换为字符串，处理各种可能的值
            result = series.copy()

            # 处理None/NaN值
            mask_none = result.isna()

            # 将非None值转换为字符串
            result = result.astype(str)

            # 恢复None值
            result[mask_none] = None

            # 按配置决定是否将特定 tokens 转换为 None（默认保留字面量，不转换）
            if not self.preserve_string_null_tokens:
                tokens = self.string_null_tokens
                # 同时支持大小写不敏感匹配：先构造小写映射
                lower_map = {str(t).lower() for t in tokens}
                result = result.apply(lambda v: None if (isinstance(v, str) and v.strip().lower() in lower_map) else v)

            logger.debug(f"列 {column_name} 强制保持为字符串类型，示例值: {result.head().tolist()}")
            return result

        # 处理日期时间类型
        elif any(t in maxcompute_type for t in ['datetime', 'timestamp', 'date']):
            # 保持原始格式，让BigQuery处理
            logger.debug(f"列 {column_name} 保持原始格式（源表为日期时间类型）")
            return series

        # 其他类型保持原样
        else:
            logger.debug(f"列 {column_name} 保持原始类型（未知的MaxCompute类型: {maxcompute_type}）")
            return series

    def _basic_dataframe_cleanup(self, df):
        """
        基础的DataFrame清理，不进行类型推断

        Args:
            df: 原始DataFrame

        Returns:
            清理后的DataFrame
        """
        cleaned_df = df.copy()

        for column in cleaned_df.columns:
            # 只进行基础清理，不改变数据类型
            if cleaned_df[column].dtype == 'object':
                # 字符串列默认不把字面量空值标记替换为 None，保留字面量
                pass
            elif cleaned_df[column].dtype in ['float64', 'float32']:
                # 清理浮点数的无穷大值
                cleaned_df[column] = cleaned_df[column].replace([float('inf'), float('-inf')], None)

        return cleaned_df

    def _apply_mysql_defaults(self, df: pd.DataFrame, destination_schema: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        根据MySQL目标表的非空约束和默认值，填充DataFrame中的NULL值。
        
        Args:
            df: 原始DataFrame
            destination_schema: 目标表的schema信息，包含is_nullable和column_default
            
        Returns:
            填充了默认值的DataFrame
        """
        modified_df = df.copy()
        
        # 创建从列名到其默认值和可空性的映射
        mysql_column_info = {col['name'].lower(): col for col in destination_schema}

        for col_name in modified_df.columns:
            lower_col_name = col_name.lower()
            if lower_col_name in mysql_column_info:
                col_info = mysql_column_info[lower_col_name]
                is_nullable = col_info.get('is_nullable')
                column_default = col_info.get('column_default')
                mysql_type = col_info.get('type')

                # 只有当列不可为空且有默认值，并且DataFrame列中包含NULL时才应用默认值
                if not is_nullable and column_default is not None and modified_df[col_name].isnull().any():
                    logger.debug(f"列 {col_name} 为非空且有默认值 '{column_default}'，填充NULL值")
                    
                    # 尝试将默认值转换为适当的类型
                    fill_value = column_default
                    try:
                        lower_type = (mysql_type or '').lower()
                        if any(t in lower_type for t in ['int', 'bigint', 'tinyint', 'smallint']):
                            fill_value = int(str(column_default))
                        elif any(t in lower_type for t in ['float', 'double', 'decimal']):
                            fill_value = float(str(column_default))
                        elif 'boolean' in lower_type:
                            # 假设布尔值默认是0或1
                            fill_value = bool(int(str(column_default)))
                        elif 'bit' in lower_type:
                            # 处理 BIT(1) 等位类型默认值，例如 b'0' / b'1' / 0x00 / 0 / 1
                            ds = str(column_default).strip()
                            if ds.startswith("b'") and ds.endswith("'"):
                                inner = ds[2:-1]
                                if inner in ('0', '1'):
                                    fill_value = int(inner)
                                else:
                                    # 尝试按十六进制或十进制解析
                                    try:
                                        fill_value = int(inner, 0)
                                    except Exception:
                                        fill_value = 0
                            elif ds.lower().startswith('0x'):
                                try:
                                    val = int(ds, 16)
                                    fill_value = 1 if val != 0 else 0
                                except Exception:
                                    fill_value = 0
                            elif ds in ('0', '1'):
                                fill_value = int(ds)
                            else:
                                try:
                                    fill_value = int(ds)
                                except Exception:
                                    fill_value = 0
                        elif any(t in lower_type for t in ['date', 'time', 'timestamp']):
                            # 处理时间表达式默认值
                            default_str = str(column_default).strip().upper()
                            if default_str in ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP()', 'NOW()', 'NOW']:
                                fill_value = pd.Timestamp.utcnow().to_pydatetime()
                            else:
                                # 尝试解析具体时间字符串，否则保留原值让驱动处理
                                try:
                                    parsed = pd.to_datetime(column_default, errors='raise')
                                    fill_value = parsed.to_pydatetime()
                                except Exception:
                                    fill_value = column_default
                        else:
                            # 字符串等类型直接使用原始默认值
                            fill_value = column_default
                    except ValueError:
                        logger.warning(f"无法将默认值 '{column_default}' 转换为列 {col_name} 的类型 {mysql_type}，保持原始字符串")

                    modified_df[col_name] = modified_df[col_name].fillna(fill_value)
        return modified_df

    def _dedupe_mysql_schema(self, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        对MySQL目标schema按小写列名去重，保留首次出现；记录被丢弃的重复列。
        """
        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        dropped: List[str] = []
        for col in schema:
            name = col.get('name')
            if not name:
                continue
            lower = name.lower()
            if lower in seen:
                dropped.append(name)
                continue
            seen.add(lower)
            deduped.append(col)
        if dropped:
            logger.warning(f"MySQL建表去重：丢弃重复列 {', '.join(dropped)}")
        return deduped

    def _validate_non_nullable_columns_before_write(self, df: pd.DataFrame, destination_schema: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        在写入前校验非空列中的空值，根据策略 fail/fill/skip 处理。
        仅对字符串/日期列在 fill 时使用哨兵值；数值列遇到空值仍失败。
        """
        if df is None or df.empty:
            return df

        policy = self.null_on_non_nullable
        if policy not in ['fail', 'fill', 'skip']:
            policy = 'fail'

        # 构建列信息映射
        col_info_map = {c['name'].lower(): c for c in destination_schema}
        non_nullable_cols = [c['name'] for c in destination_schema if not c.get('is_nullable', True)]

        if not non_nullable_cols:
            return df

        working_df = df.copy()
        rows_before = len(working_df)

        null_violations = {}
        for col in working_df.columns:
            info = col_info_map.get(col.lower())
            if not info:
                continue
            if not info.get('is_nullable', True):
                # 统计空值
                null_mask = working_df[col].isnull()
                null_count = int(null_mask.sum())
                if null_count > 0:
                    null_violations[col] = null_count
                    if policy == 'fill':
                        mysql_type = (info.get('type') or '').lower()
                        # 仅字符串/日期列填充
                        if any(t in mysql_type for t in ['char', 'text', 'blob', 'date', 'time', 'year']):
                            sentinel = self.null_fill_sentinel if self.null_fill_sentinel is not None else ''
                            working_df.loc[null_mask, col] = sentinel
                        else:
                            # 数值列不自动填充，仍按 fail 处理
                            pass
                    elif policy == 'skip':
                        # 将在循环结束统一过滤
                        pass

        if null_violations:
            if policy == 'fail':
                details = ', '.join([f"{k}={v}" for k, v in null_violations.items()])
                raise DataMigrationError(f"写入前校验失败：非空列包含NULL。详情: {details}")
            if policy == 'skip':
                # 过滤含任一非空列空值的行
                mask_keep = pd.Series(True, index=working_df.index)
                for col, _ in null_violations.items():
                    mask_keep &= working_df[col].notnull()
                dropped = int((~mask_keep).sum())
                if dropped > 0:
                    logger.warning(f"根据策略 skip，过滤掉 {dropped} 行包含非空列NULL的记录")
                working_df = working_df[mask_keep]

        rows_after = len(working_df)
        if rows_after != rows_before:
            logger.info(f"写入前校验处理完成：行数 {rows_before} -> {rows_after}")
        return working_df

    def _prepare_mysql_schema_inputs(self, source_columns: List[Dict[str, Any]], mapping: Dict[str, Any]):
        """
        基于映射构建用于生成 MySQL 目标表结构的列定义：
        - 过滤分区列
        - include/exclude 选择
        - rename 应用到列名
        - 追加 computed 列（默认 string，可被 overrides 覆盖）
        - 按 order 排列
        返回: (prepared_columns, overrides_by_target_name, final_target_names)
        """
        # 取非分区源列
        base_cols = [
            {"name": c['name'], "type": c['type'], "is_partition": False}
            for c in source_columns if not c.get('is_partition', False)
        ]
        include = mapping.get('include') or []
        exclude = mapping.get('exclude') or []
        rename = mapping.get('rename') or {}
        type_override = mapping.get('type_override') or {}
        computed = mapping.get('computed') or {}
        order = mapping.get('order') or []

        lower_name_to_col = {c['name'].lower(): c for c in base_cols}

        # include / exclude
        if include:
            selected = [lower_name_to_col[n.lower()] for n in include if n.lower() in lower_name_to_col]
        else:
            selected = list(base_cols)
        if exclude:
            selected = [c for c in selected if c['name'].lower() not in {n.lower() for n in exclude}]

        # rename 应用到列定义
        prepared: List[Dict[str, Any]] = []
        for c in selected:
            # 支持大小写不敏感的 rename 键
            new_name = None
            if c['name'] in rename:
                new_name = rename[c['name']]
            else:
                for k, v in rename.items():
                    if str(k).lower() == c['name'].lower():
                        new_name = v
                        break
            if new_name:
                prepared.append({"name": str(new_name), "type": c['type'], "is_partition": False})
            else:
                prepared.append({"name": c['name'], "type": c['type'], "is_partition": False})

        # 追加 computed 列（默认 string）
        for comp_name in (computed.keys() if isinstance(computed, dict) else []):
            if not any(col['name'].lower() == str(comp_name).lower() for col in prepared):
                prepared.append({"name": str(comp_name), "type": "string", "is_partition": False})

        # 按 order 排列
        if order:
            order_lower = [str(n).lower() for n in order]
            ordered = [col for name in order_lower for col in prepared if col['name'].lower() == name]
            remaining = [col for col in prepared if col['name'].lower() not in order_lower]
            prepared = ordered + remaining

        # overrides（按目标列名）
        overrides: Dict[str, str] = {}
        if isinstance(type_override, dict):
            for k, v in type_override.items():
                overrides[str(k)] = v

        final_names = [c['name'] for c in prepared]
        return prepared, overrides, final_names

    # ---------- 映射：仅 MySQL 一期 ----------
    def _validate_mapping_mysql(self, mapping: Dict[str, Any], source_columns: List[Dict[str, Any]]) -> None:
        """
        校验 include/exclude/rename/type_override/computed 是否可用。
        仅进行基础校验：源列存在、目标列不重复、函数白名单。
        """
        source_names = {c['name'].lower() for c in source_columns if not c.get('is_partition', False)}
        include = mapping.get('include') or []
        exclude = mapping.get('exclude') or []
        rename = mapping.get('rename') or {}
        type_override = mapping.get('type_override') or {}
        computed = mapping.get('computed') or {}
        order = mapping.get('order') or []

        # include/exclude 列存在性（大小写不敏感）
        for col in include:
            if col.lower() not in source_names:
                raise DataMigrationError(f"映射 include 的列不存在于源表: {col}")
        for col in exclude:
            # 可允许排除不存在列，不报错
            pass

        # rename: 源列必须存在；目标列不能重复
        target_names = set()
        for src, dst in rename.items():
            if src.lower() not in source_names:
                raise DataMigrationError(f"映射 rename 的源列不存在: {src}")
            dst_lower = str(dst).lower()
            if dst_lower in target_names:
                raise DataMigrationError(f"映射 rename 产生重复目标列: {dst}")
            target_names.add(dst_lower)

        # computed 函数白名单
        for dst, spec in computed.items():
            if isinstance(spec, dict):
                func = str(spec.get('func', '')).lower()
                if func not in {'concat', 'upper', 'lower', 'substr', 'now', 'format'}:
                    raise DataMigrationError(f"computed 不支持的函数: {func}")
                # format 特殊校验
                if func == 'format':
                    args = spec.get('args') or []
                    if not args or not isinstance(args[0], str):
                        raise DataMigrationError("format 需要以模板字符串为第一个参数")
                    template = str(args[0])
                    # 构建可用列集合（源列、rename 目标、computed 目标）
                    available_cols = set(source_names)
                    available_cols.update({str(v).lower() for v in rename.values()})
                    available_cols.update({str(k).lower() for k in computed.keys()})
                    # 命名占位符: 仅一个参数
                    if len(args) == 1:
                        try:
                            needed = set()
                            for lit, field_name, fmt, conv in string.Formatter().parse(template):
                                if field_name is not None and field_name != '':
                                    needed.add(str(field_name).lower())
                            # 校验存在性（允许纯字面量模板）
                            for name in needed:
                                if name not in available_cols:
                                    raise DataMigrationError(f"format 模板引用了不存在的列: {name}")
                        except ValueError:
                            raise DataMigrationError("format 模板解析失败，请检查花括号匹配")
                    else:
                        # 位置占位符: args[1:] 为列名或字面量
                        for a in args[1:]:
                            if isinstance(a, str):
                                # 若作为列名存在则校验，否则视为字面量
                                if a.lower() in available_cols:
                                    continue
                                # 不是列名则认为是字面量，跳过
                            # 非字符串参数视为字面量
                            continue
            else:
                raise DataMigrationError("computed 需使用 JSON 对象形式 {func, args}")

        # order 不做严格校验，运行时按存在列重排

    def _transform_dataframe_by_mapping_mysql(self, df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
        transformed = df.copy()

        include = mapping.get('include') or []
        exclude = mapping.get('exclude') or []
        rename = mapping.get('rename') or {}
        defaults = mapping.get('defaults') or {}
        computed = mapping.get('computed') or {}
        order = mapping.get('order') or []

        # include / exclude
        src_cols_lower = {c.lower(): c for c in transformed.columns}
        if include:
            cols = [src_cols_lower[c.lower()] for c in include if c.lower() in src_cols_lower]
            transformed = transformed[cols]
        if exclude:
            drop_cols = [src_cols_lower[c.lower()] for c in exclude if c.lower() in src_cols_lower]
            transformed = transformed.drop(columns=[c for c in drop_cols if c in transformed.columns])

        # rename（大小写不敏感）
        rename_map = {}
        for src, dst in rename.items():
            if src.lower() in src_cols_lower:
                rename_map[src_cols_lower[src.lower()]] = dst
        if rename_map:
            transformed = transformed.rename(columns=rename_map)

        # computed
        for dst, spec in computed.items():
            if not isinstance(spec, dict):
                continue
            func = str(spec.get('func', '')).lower()
            args = spec.get('args') or []
            transformed[dst] = self._evaluate_computed(transformed, func, args)

        # defaults（应用层）
        for col, val in defaults.items():
            if col in transformed.columns:
                transformed[col] = transformed[col].fillna(val)

        # order：仅对存在列生效，缺失的忽略
        if order:
            ordered = [c for c in order if c in transformed.columns]
            remaining = [c for c in transformed.columns if c not in ordered]
            transformed = transformed[ordered + remaining]

        return transformed

    def _evaluate_computed(self, df: pd.DataFrame, func: str, args: List[Any]):
        func = (func or '').lower()
        if func == 'now':
            return pd.Timestamp.utcnow()
        if func == 'concat':
            # 字符串连接，支持字面量
            series_list: List[pd.Series] = []
            for a in args:
                if isinstance(a, str) and a in df.columns:
                    series_list.append(df[a].astype(str))
                else:
                    lit = pd.Series([str(a)] * len(df), index=df.index)
                    series_list.append(lit)
            if not series_list:
                return pd.Series([""] * len(df), index=df.index)
            out = series_list[0]
            for s in series_list[1:]:
                out = out + s
            return out
        if func == 'upper':
            col = args[0]
            return df[col].astype(str).str.upper()
        if func == 'lower':
            col = args[0]
            return df[col].astype(str).str.lower()
        if func == 'substr':
            col = args[0]
            start = int(args[1]) if len(args) > 1 else 0
            length = int(args[2]) if len(args) > 2 else None
            s = df[col].astype(str)
            return s.str.slice(start, start + length if length is not None else None)
        if func == 'format':
            # 支持两种形式：
            # 1) 命名占位符: args = ["{year}-{week:02d}"]
            # 2) 位置占位符: args = ["{}-{:02d}", "year", "week"]
            if not args or not isinstance(args[0], str):
                return pd.Series([None] * len(df), index=df.index)
            template = str(args[0])
            formatter = string.Formatter()
            # 解析模板以识别占位符顺序与 numeric d 格式
            fields = []  # List[Tuple[field_name, format_spec]]; field_name may be '' for positional
            try:
                for lit, field_name, fmt, conv in formatter.parse(template):
                    if field_name is not None:
                        fields.append((str(field_name), str(fmt or '')))
            except ValueError:
                return pd.Series([None] * len(df), index=df.index)

            def coerce_value(val, fmt_spec: str):
                # None/NaN 处理：数字 d 用 0；否则空字符串
                is_nan = val is None or (isinstance(val, float) and np.isnan(val))
                if 'd' in fmt_spec:  # 期望整数
                    if is_nan or (isinstance(val, str) and val.strip() == ''):
                        return 0
                    try:
                        return int(val)
                    except Exception:
                        return 0
                else:
                    if is_nan:
                        return ''
                    return val

            if len(args) == 1:
                # 命名占位符
                def fmt_row(row):
                    data = {}
                    for fname, fmt_spec in fields:
                        if fname == '':
                            # 位置占位符在命名模式中忽略
                            continue
                        v = row.get(fname, None)
                        data[fname] = coerce_value(v, fmt_spec)
                    try:
                        return template.format(**data)
                    except Exception:
                        return ''
                return df.apply(lambda r: fmt_row(r), axis=1)
            else:
                # 位置占位符
                sources = args[1:]
                # 预先解析每个位置是否 d 格式
                numeric_flags = []
                for fname, fmt_spec in fields:
                    numeric_flags.append('d' in fmt_spec)
                def fmt_row(row):
                    vals = []
                    for i, src in enumerate(sources):
                        if isinstance(src, str) and src in row.index:
                            v = row[src]
                        else:
                            v = src  # 字面量
                        fmt_spec = ''
                        if i < len(numeric_flags) and numeric_flags[i]:
                            fmt_spec = 'd'
                        vals.append(coerce_value(v, fmt_spec))
                    try:
                        return template.format(*vals)
                    except Exception:
                        return ''
                return df.apply(lambda r: fmt_row(r), axis=1)
        # 未知函数返回空列
        return None

    def generate_mysql_mapping_summary(self, source_columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        生成 MySQL 映射摘要（仅在存在映射计划时返回）。
        返回包含 include/exclude/rename/computed/type_override/order 以及最终列名与数量的字典。
        """
        if self.destination_type != 'mysql' or not self.column_mapping_plan:
            return None
        try:
            # 校验并准备
            self._validate_mapping_mysql(self.column_mapping_plan, source_columns)
            prepared_cols, overrides, final_names = self._prepare_mysql_schema_inputs(source_columns, self.column_mapping_plan)

            mapping = self.column_mapping_plan
            include = mapping.get('include') or []
            exclude = mapping.get('exclude') or []
            rename = mapping.get('rename') or {}
            computed = list((mapping.get('computed') or {}).keys())
            type_override = overrides or {}
            order = mapping.get('order') or []

            return {
                'include': include,
                'exclude': exclude,
                'rename': rename,
                'computed': computed,
                'type_override': type_override,
                'order': order,
                'final_columns': final_names,
                'final_count': len(final_names),
            }
        except Exception as e:
            logger.debug(f"生成映射摘要失败: {e}")
            return None

    
