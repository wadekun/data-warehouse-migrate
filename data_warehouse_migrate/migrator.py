"""
数据迁移核心模块
"""

from typing import Optional, List
from enum import Enum
from tqdm import tqdm
import pandas as pd
import numpy as np
from google.cloud import bigquery

from .maxcompute_client import MaxComputeClient
from .bigquery_client import BigQueryClient
from .schema_mapper import SchemaMapper
from .exceptions import DataMigrationError
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
                 destination_project_id: str,
                 maxcompute_access_id: Optional[str] = None,
                 maxcompute_secret_key: Optional[str] = None,
                 maxcompute_endpoint: Optional[str] = None,
                 bigquery_credentials_path: Optional[str] = None):
        """
        初始化数据迁移器
        
        Args:
            source_project_id: MaxCompute项目ID
            destination_project_id: BigQuery项目ID
            maxcompute_access_id: MaxCompute AccessKey ID
            maxcompute_secret_key: MaxCompute AccessKey Secret
            maxcompute_endpoint: MaxCompute endpoint
            bigquery_credentials_path: BigQuery凭证文件路径
        """
        self.source_project_id = source_project_id
        self.destination_project_id = destination_project_id
        
        # 初始化客户端
        self.maxcompute_client = MaxComputeClient(
            access_id=maxcompute_access_id,
            secret_access_key=maxcompute_secret_key,
            endpoint=maxcompute_endpoint,
            project=source_project_id
        )
        
        self.bigquery_client = BigQueryClient(
            project_id=destination_project_id,
            credentials_path=bigquery_credentials_path
        )
        
        self.schema_mapper = SchemaMapper()
        self._source_schema_cache = {}  # 缓存源表结构
        self._bigquery_schema_cache = {}  # 缓存BigQuery表结构
    
    def migrate_table(self, 
                     source_table_name: str,
                     destination_dataset_id: str,
                     destination_table_name: str,
                     mode: MigrationMode = MigrationMode.APPEND,
                     batch_size: int = 10000) -> None:
        """
        迁移表数据
        
        Args:
            source_table_name: 源表名
            destination_dataset_id: 目标数据集ID
            destination_table_name: 目标表名
            mode: 迁移模式
            batch_size: 批次大小
        """
        try:
            logger.info(f"开始迁移表: {source_table_name} -> {destination_dataset_id}.{destination_table_name}")
            
            # 1. 测试连接
            self._test_connections()
            
            # 2. 创建数据集（如果不存在）
            self.bigquery_client.create_dataset_if_not_exists(destination_dataset_id)
            
            # 3. 处理表结构
            self._handle_table_schema(
                source_table_name, 
                destination_dataset_id, 
                destination_table_name, 
                mode
            )
            
            # 4. 迁移数据
            self._migrate_table_data(
                source_table_name,
                destination_dataset_id,
                destination_table_name,
                mode,
                batch_size
            )
            
            logger.info(f"表迁移完成: {source_table_name} -> {destination_dataset_id}.{destination_table_name}")
            
        except Exception as e:
            logger.error(f"表迁移失败: {e}")
            raise DataMigrationError(f"表迁移失败: {e}")
    
    def _test_connections(self) -> None:
        """测试连接"""
        logger.info("测试数据库连接...")
        
        if not self.maxcompute_client.test_connection():
            raise DataMigrationError("MaxCompute连接失败")
        
        if not self.bigquery_client.test_connection():
            raise DataMigrationError("BigQuery连接失败")
        
        logger.info("数据库连接测试通过")
    
    def _handle_table_schema(self, 
                           source_table_name: str,
                           destination_dataset_id: str,
                           destination_table_name: str,
                           mode: MigrationMode) -> None:
        """处理表结构"""
        logger.info("处理表结构...")
        
        # 检查目标表是否存在
        table_exists = self.bigquery_client.table_exists(
            destination_dataset_id, 
            destination_table_name
        )
        
        if table_exists:
            if mode == MigrationMode.OVERWRITE:
                logger.info("目标表已存在，删除并重新创建")
                self.bigquery_client.delete_table(
                    destination_dataset_id, 
                    destination_table_name
                )
                table_exists = False
            else:
                logger.info("目标表已存在，将追加数据")
        
        if not table_exists:
            # 验证源表访问权限（特别是分区表）
            logger.info(f"验证源表 {source_table_name} 的访问权限")
            if not self.maxcompute_client.validate_table_access(source_table_name):
                raise DataMigrationError(f"无法访问源表 {source_table_name}，请检查表是否存在或分区配置")

            # 获取源表结构
            logger.info(f"获取源表 {source_table_name} 的结构")
            maxcompute_columns = self.maxcompute_client.get_table_schema(source_table_name)

            # 转换为BigQuery结构
            logger.info("转换表结构")
            bigquery_schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(
                maxcompute_columns
            )

            # 创建目标表
            logger.info(f"创建目标表 {destination_dataset_id}.{destination_table_name}")
            self.bigquery_client.create_table(
                destination_dataset_id,
                destination_table_name,
                bigquery_schema,
                f"从MaxCompute表 {source_table_name} 迁移"
            )
    
    def _migrate_table_data(self, 
                          source_table_name: str,
                          destination_dataset_id: str,
                          destination_table_name: str,
                          mode: MigrationMode,
                          batch_size: int) -> None:
        """迁移表数据"""
        logger.info("开始迁移数据...")
        
        # 确定写入模式
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE 
            if mode == MigrationMode.OVERWRITE 
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        
        # 分批读取和写入数据
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

                # 根据源表结构进行数据类型处理
                typed_df = self._apply_source_schema_types(batch_df, source_table_name)

                # 加载数据到BigQuery
                self.bigquery_client.load_data_from_dataframe(
                    destination_dataset_id,
                    destination_table_name,
                    typed_df,
                    write_disposition,
                    table_schema=self._get_bigquery_schema(source_table_name)
                )
                
                # 第一批后改为追加模式
                if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
                    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            
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

            # 清理字符串表示的None值
            result = result.replace(['nan', 'None', 'null', '<NA>', 'NaN'], None)

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
                # 清理字符串类型的特殊值
                cleaned_df[column] = cleaned_df[column].replace(['nan', 'None', 'null', '<NA>'], None)
            elif cleaned_df[column].dtype in ['float64', 'float32']:
                # 清理浮点数的无穷大值
                cleaned_df[column] = cleaned_df[column].replace([float('inf'), float('-inf')], None)

        return cleaned_df

    def _optimize_dataframe_types(self, df):
        """
        优化DataFrame的数据类型，确保与BigQuery兼容

        Args:
            df: 原始DataFrame

        Returns:
            优化后的DataFrame
        """
        import pandas as pd
        import numpy as np

        optimized_df = df.copy()

        for column in optimized_df.columns:
            try:
                # 获取列的数据类型
                dtype = optimized_df[column].dtype

                # 处理object类型（通常是字符串或混合类型）
                if dtype == 'object':
                    # 首先检查源表的数据类型信息，避免错误的类型推断
                    should_convert_to_numeric = self._should_convert_to_numeric(column, optimized_df[column])

                    if should_convert_to_numeric:
                        # 检查是否可以转换为数值类型
                        sample_values = optimized_df[column].dropna().head(100)
                        if len(sample_values) > 0:
                            # 尝试转换为数值
                            try:
                                # 检查是否都是数字字符串
                                numeric_values = pd.to_numeric(sample_values, errors='coerce')
                                if not numeric_values.isna().all():
                                    # 如果大部分值可以转换为数字
                                    if numeric_values.notna().sum() / len(sample_values) > 0.9:  # 提高阈值到90%
                                        # 检查是否为整数
                                        if all(float(x).is_integer() for x in numeric_values.dropna()):
                                            # 转换为数值类型，但使用标准的int64/float64
                                            converted_series = pd.to_numeric(optimized_df[column], errors='coerce')
                                            if converted_series.isna().any():
                                                # 有NaN值时使用float64
                                                optimized_df[column] = converted_series.astype('float64')
                                                logger.debug(f"列 {column} 转换为float64类型（包含NaN）")
                                            else:
                                                # 没有NaN值时使用int64
                                                optimized_df[column] = converted_series.astype('int64')
                                                logger.debug(f"列 {column} 转换为int64类型")
                                        else:
                                            optimized_df[column] = pd.to_numeric(optimized_df[column], errors='coerce').astype('float64')
                                            logger.debug(f"列 {column} 转换为float64类型")
                                        continue
                            except:
                                pass

                    # 保持为字符串类型，但清理特殊值
                    optimized_df[column] = optimized_df[column].astype(str)
                    optimized_df[column] = optimized_df[column].replace(['nan', 'None', 'null', '<NA>'], None)
                    logger.debug(f"列 {column} 保持为字符串类型")

                # 处理整数类型，转换为float64以兼容pyarrow
                elif dtype in ['int64', 'int32', 'int16', 'int8']:
                    # 检查是否有NaN值
                    if optimized_df[column].isna().any():
                        # 有NaN值时转换为float64
                        optimized_df[column] = optimized_df[column].astype('float64')
                        logger.debug(f"列 {column} 包含NaN值，转换为float64类型")
                    else:
                        # 没有NaN值时保持int64
                        optimized_df[column] = optimized_df[column].astype('int64')
                        logger.debug(f"列 {column} 保持int64类型")

                # 处理浮点数类型，清理无穷大值
                elif dtype in ['float64', 'float32']:
                    optimized_df[column] = optimized_df[column].replace([np.inf, -np.inf], None)

                # 处理布尔类型，使用标准bool类型
                elif dtype == 'bool':
                    optimized_df[column] = optimized_df[column].astype('bool')

            except Exception as e:
                logger.warning(f"优化列 {column} 的数据类型时出错: {e}")
                # 如果优化失败，确保至少是字符串类型
                try:
                    optimized_df[column] = optimized_df[column].astype(str)
                    optimized_df[column] = optimized_df[column].replace(['nan', 'None', 'null'], None)
                except:
                    pass

        return optimized_df

    def _get_bigquery_schema(self, source_table_name: str) -> List[bigquery.SchemaField]:
        """
        获取BigQuery表结构
        
        Args:
            source_table_name: 源表名
            
        Returns:
            BigQuery SchemaField列表
        """
        try:
            # 使用缓存的BigQuery表结构
            if source_table_name not in self._bigquery_schema_cache:
                # 如果缓存中没有，获取源表结构并转换
                source_columns = self.maxcompute_client.get_table_schema(source_table_name)
                
                # 转换为BigQuery结构
                bigquery_schema = self.schema_mapper.convert_maxcompute_to_bigquery_schema(
                    source_columns
                )
                
                # 缓存BigQuery表结构
                self._bigquery_schema_cache[source_table_name] = bigquery_schema
                logger.info(f"缓存BigQuery表结构 {source_table_name}，共 {len(bigquery_schema)} 列")
            else:
                bigquery_schema = self._bigquery_schema_cache[source_table_name]
                logger.debug(f"使用缓存的BigQuery表结构 {source_table_name}")
            
            return bigquery_schema
            
        except Exception as e:
            logger.error(f"获取BigQuery表结构失败: {e}")
            return None
