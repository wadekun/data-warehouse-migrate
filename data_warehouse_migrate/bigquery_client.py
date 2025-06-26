"""
BigQuery客户端模块
"""

from typing import List, Optional
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import config
from .exceptions import BigQueryConnectionError, TableNotFoundError
from .logger import setup_logger

logger = setup_logger(__name__)


class BigQueryClient:
    """BigQuery客户端"""
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """
        初始化BigQuery客户端
        
        Args:
            project_id: GCP项目ID
            credentials_path: 服务账号凭证文件路径
        """
        self.project_id = project_id
        self.credentials_path = credentials_path or config.bigquery_credentials_path
        
        try:
            if self.credentials_path:
                self.client = bigquery.Client.from_service_account_json(
                    self.credentials_path, project=self.project_id
                )
            else:
                self.client = bigquery.Client(project=self.project_id)
            
            logger.info(f"成功连接到BigQuery项目: {self.project_id}")
            
        except Exception as e:
            logger.error(f"连接BigQuery失败: {e}")
            raise BigQueryConnectionError(f"连接BigQuery失败: {e}")
    
    def create_dataset_if_not_exists(self, dataset_id: str, location: str = "US") -> None:
        """
        创建数据集（如果不存在）
        
        Args:
            dataset_id: 数据集ID
            location: 数据集位置
        """
        try:
            dataset_ref = self.client.dataset(dataset_id)
            self.client.get_dataset(dataset_ref)
            logger.info(f"数据集 {dataset_id} 已存在")
        except NotFound:
            # 数据集不存在，创建新的
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"成功创建数据集: {dataset_id}")
        except Exception as e:
            logger.error(f"处理数据集时出错: {e}")
            raise BigQueryConnectionError(f"处理数据集 {dataset_id} 时出错: {e}")
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        检查表是否存在
        
        Args:
            dataset_id: 数据集ID
            table_id: 表ID
            
        Returns:
            表是否存在
        """
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"检查表存在性时出错: {e}")
            raise BigQueryConnectionError(f"检查表 {dataset_id}.{table_id} 存在性时出错: {e}")
    
    def create_table(self, dataset_id: str, table_id: str, 
                    schema: List[bigquery.SchemaField], 
                    description: Optional[str] = None) -> None:
        """
        创建表
        
        Args:
            dataset_id: 数据集ID
            table_id: 表ID
            schema: 表结构
            description: 表描述
        """
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            
            if description:
                table.description = description
            
            table = self.client.create_table(table)
            logger.info(f"成功创建表: {dataset_id}.{table_id}")
            
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            raise BigQueryConnectionError(f"创建表 {dataset_id}.{table_id} 失败: {e}")
    
    def delete_table(self, dataset_id: str, table_id: str) -> None:
        """
        删除表
        
        Args:
            dataset_id: 数据集ID
            table_id: 表ID
        """
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.delete_table(table_ref)
            logger.info(f"成功删除表: {dataset_id}.{table_id}")
            
        except NotFound:
            logger.warning(f"表 {dataset_id}.{table_id} 不存在，无需删除")
        except Exception as e:
            logger.error(f"删除表失败: {e}")
            raise BigQueryConnectionError(f"删除表 {dataset_id}.{table_id} 失败: {e}")
    
    def load_data_from_dataframe(self, dataset_id: str, table_id: str,
                                dataframe: pd.DataFrame,
                                write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
                                schema: dict = None) -> None:
        """
        从DataFrame加载数据到表

        Args:
            dataset_id: 数据集ID
            table_id: 表ID
            dataframe: 数据DataFrame
            write_disposition: 写入模式
            schema: 字段名到bigquery类型的映射（可选）
        """
        try:
            # 清理DataFrame数据类型
            cleaned_df = self._clean_dataframe_for_bigquery(dataframe, schema)

            # 确保与pyarrow兼容
            pyarrow_compatible_df = self._ensure_pyarrow_compatibility(cleaned_df)

            # 验证pyarrow转换
            self._validate_pyarrow_conversion(pyarrow_compatible_df)

            table_ref = self.client.dataset(dataset_id).table(table_id)

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = write_disposition
            job_config.autodetect = False  # 使用预定义的schema

            # 添加更多配置来处理数据类型问题
            job_config.allow_quoted_newlines = True
            job_config.allow_jagged_rows = False
            job_config.ignore_unknown_values = False

            job = self.client.load_table_from_dataframe(
                pyarrow_compatible_df, table_ref, job_config=job_config
            )

            job.result()  # 等待作业完成

            logger.info(f"成功加载 {len(pyarrow_compatible_df)} 行数据到表 {dataset_id}.{table_id}")

        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            # 提供更详细的错误信息
            if "Could not convert" in str(e):
                logger.error("数据类型转换错误，请检查源数据格式")
                logger.error(f"DataFrame数据类型: {dataframe.dtypes.to_dict()}")

                # 尝试分析具体的转换问题
                self._analyze_conversion_error(str(e), dataframe)

            raise BigQueryConnectionError(f"加载数据到表 {dataset_id}.{table_id} 失败: {e}")

    def _analyze_conversion_error(self, error_msg: str, df: pd.DataFrame) -> None:
        """
        分析数据类型转换错误

        Args:
            error_msg: 错误消息
            df: DataFrame
        """
        try:
            # 查找问题列
            for column in df.columns:
                if df[column].dtype == 'object':
                    # 检查是否有混合类型
                    sample_values = df[column].dropna().head(20).tolist()
                    value_types = [type(v).__name__ for v in sample_values]
                    unique_types = set(value_types)

                    if len(unique_types) > 1:
                        logger.error(f"列 {column} 包含混合数据类型: {unique_types}")
                        logger.error(f"示例值: {sample_values[:5]}")

                    # 检查是否有特殊字符
                    str_values = [str(v) for v in sample_values if v is not None]
                    for val in str_values[:5]:
                        if any(ord(c) > 127 for c in str(val)):
                            logger.error(f"列 {column} 包含特殊字符: {repr(val)}")
                            break

        except Exception as e:
            logger.debug(f"分析转换错误时出错: {e}")

    def _clean_dataframe_for_bigquery(self, df: pd.DataFrame, schema: dict = None) -> pd.DataFrame:
        """
        清理DataFrame以确保与BigQuery兼容

        Args:
            df: 原始DataFrame
            schema: 字段名到bigquery类型的映射（可选）

        Returns:
            清理后的DataFrame
        """
        cleaned_df = df.copy()

        for column in cleaned_df.columns:
            col_type = None
            if schema and column in schema:
                col_type = schema[column].upper()
            # 只对schema为数值类型的列尝试转数值
            if col_type in ["INT64", "FLOAT64", "NUMERIC"]:
                try:
                    cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors='coerce')
                    continue
                except:
                    pass
            # 其余全部强制为字符串
            cleaned_df[column] = cleaned_df[column].astype(str)
            cleaned_df[column] = cleaned_df[column].replace(['nan', 'None', 'null'], None)
        return cleaned_df

    def _ensure_pyarrow_compatibility(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        确保DataFrame与pyarrow兼容

        Args:
            df: 输入DataFrame

        Returns:
            与pyarrow兼容的DataFrame
        """
        import numpy as np

        compatible_df = df.copy()

        for column in compatible_df.columns:
            try:
                dtype = str(compatible_df[column].dtype)

                # 首先处理所有浮点数列的特殊值
                if compatible_df[column].dtype in ['float64', 'float32']:
                    # 清理无穷大值和特殊浮点值
                    compatible_df[column] = compatible_df[column].replace([np.inf, -np.inf], None)
                    # 确保所有值都是有效的浮点数
                    compatible_df[column] = pd.to_numeric(compatible_df[column], errors='coerce')
                    logger.debug(f"列 {column} 清理了特殊浮点值")

                # 处理pandas的nullable类型
                elif dtype == 'Int64':
                    if compatible_df[column].isna().any():
                        compatible_df[column] = pd.to_numeric(compatible_df[column], errors='coerce').astype('float64')
                        logger.debug(f"列 {column} 从Int64转换为float64（包含NaN）")
                    else:
                        compatible_df[column] = compatible_df[column].astype('int64')
                        logger.debug(f"列 {column} 从Int64转换为int64")

                elif dtype in ['Int32', 'Int16', 'Int8']:
                    if compatible_df[column].isna().any():
                        compatible_df[column] = pd.to_numeric(compatible_df[column], errors='coerce').astype('float64')
                    else:
                        compatible_df[column] = compatible_df[column].astype('int64')

                elif dtype == 'boolean':
                    # 处理nullable boolean
                    compatible_df[column] = compatible_df[column].fillna(False).astype('bool')
                    logger.debug(f"列 {column} 从boolean转换为bool")

                elif dtype == 'string':
                    # 处理pandas string类型
                    compatible_df[column] = compatible_df[column].astype('object')
                    logger.debug(f"列 {column} 从string转换为object")

                # 处理object类型中可能的问题
                elif dtype == 'object':
                    # 对于object类型，强制确保为字符串类型
                    logger.debug(f"处理object类型列: {column}")

                    # 检查是否包含复杂对象
                    sample_values = compatible_df[column].dropna().head(10)
                    if len(sample_values) > 0:
                        # 强制转换为字符串类型
                        try:
                            # 保存None值的位置
                            mask_none = compatible_df[column].isna()

                            # 转换为字符串
                            compatible_df[column] = compatible_df[column].astype(str)

                            # 恢复None值
                            compatible_df.loc[mask_none, column] = None

                            # 清理字符串表示的None值
                            compatible_df[column] = compatible_df[column].replace(['nan', 'None', 'null', '<NA>', 'NaN'], None)

                            logger.debug(f"列 {column} 强制转换为字符串类型")
                        except Exception as e:
                            logger.warning(f"列 {column} 字符串转换失败: {e}，用None填充")
                            compatible_df[column] = None

                # 最后检查：确保没有复杂的嵌套结构
                if compatible_df[column].dtype == 'object':
                    # 检查是否有列表、字典等复杂类型
                    sample_values = compatible_df[column].dropna().head(5)
                    for val in sample_values:
                        if isinstance(val, (list, dict, tuple, set)):
                            logger.warning(f"列 {column} 包含复杂类型 {type(val)}，转换为字符串")
                            compatible_df[column] = compatible_df[column].astype(str)
                            break

            except Exception as e:
                logger.warning(f"处理列 {column} 时出错: {e}，尝试转换为字符串类型")
                try:
                    compatible_df[column] = compatible_df[column].astype(str)
                    compatible_df[column] = compatible_df[column].replace(['nan', 'None', 'null'], None)
                except:
                    # 最后的保险措施：用None填充整列
                    compatible_df[column] = None

        return compatible_df

    def _validate_pyarrow_conversion(self, df: pd.DataFrame) -> None:
        """
        验证DataFrame是否可以转换为pyarrow格式

        Args:
            df: 要验证的DataFrame

        Raises:
            BigQueryConnectionError: 如果转换失败
        """
        try:
            import pyarrow as pa

            # 尝试转换为pyarrow表
            table = pa.Table.from_pandas(df)
            logger.debug(f"pyarrow转换验证成功，表结构: {table.schema}")

        except ImportError:
            logger.warning("pyarrow未安装，跳过转换验证")

        except Exception as e:
            logger.error(f"pyarrow转换验证失败: {e}")

            # 尝试找出问题列
            problematic_columns = []
            for column in df.columns:
                try:
                    # 测试单列转换
                    single_col_df = df[[column]].copy()
                    pa.Table.from_pandas(single_col_df)
                except Exception as col_e:
                    problematic_columns.append((column, str(col_e)))
                    logger.error(f"列 {column} 转换失败: {col_e}")
                    logger.error(f"列 {column} 数据类型: {df[column].dtype}")
                    logger.error(f"列 {column} 示例值: {df[column].head().tolist()}")

            if problematic_columns:
                error_msg = f"以下列无法转换为pyarrow格式: {[col for col, _ in problematic_columns]}"
                raise BigQueryConnectionError(error_msg)
            else:
                raise BigQueryConnectionError(f"pyarrow转换失败: {e}")

    def test_connection(self) -> bool:
        """
        测试连接
        
        Returns:
            连接是否成功
        """
        try:
            # 尝试列出数据集
            list(self.client.list_datasets(max_results=1))
            logger.info("BigQuery连接测试成功")
            return True
        except Exception as e:
            logger.error(f"BigQuery连接测试失败: {e}")
            return False
