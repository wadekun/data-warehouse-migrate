"""
MySQL数据读取客户端
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Iterator
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)


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
        engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )
        return engine

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"MySQL连接测试成功: {self.host}:{self.port}/{self.database}")
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

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {
                    "database": self.database,
                    "table_name": table_name
                }).fetchall()

                columns = []
                for row in result:
                    # 构建类型字符串，包含精度信息
                    type_str = row.type.lower()

                    # 处理带长度的类型
                    if row.type.lower() in ['varchar', 'char'] and row.max_length:
                        type_str = f"{row.type}({row.max_length})"
                    elif row.type.lower() in ['decimal', 'numeric'] and row.numeric_precision:
                        scale = row.numeric_scale if row.numeric_scale else 0
                        type_str = f"{row.type}({row.numeric_precision},{scale})"
                    elif row.type.lower() == 'enum':
                        # 枚举类型特殊处理
                        type_str = 'string'
                    elif row.type.lower() == 'set':
                        # SET类型特殊处理
                        type_str = 'string'

                    columns.append({
                        'name': row.name,
                        'type': type_str,
                        'is_nullable': row.is_nullable == 'YES',
                        'default': row.column_default,
                        'comment': row.comment or ''
                    })

                logger.info(f"成功获取MySQL表 {table_name} 的结构，共 {len(columns)} 列")
                return columns

        except Exception as e:
            logger.error(f"获取MySQL表结构失败: {e}")
            raise

    def get_table_data(self, table_name: str,
                      batch_size: int = 10000,
                      columns: Optional[List[str]] = None,
                      where_clause: Optional[str] = None,
                      order_by: Optional[str] = None) -> Iterator[pd.DataFrame]:
        """
        流式读取表数据

        Args:
            table_name: 表名
            batch_size: 批次大小
            columns: 指定列名列表，None表示所有列
            where_clause: WHERE条件，用于增量同步
            order_by: 排序字段，用于增量同步

        Yields:
            pd.DataFrame: 批次数据
        """
        # 构建列名
        col_str = '*' if not columns else ', '.join(f"`{c}`" for c in columns)

        # 构建查询SQL
        query = f"SELECT {col_str} FROM `{table_name}`"

        # 添加WHERE条件
        if where_clause:
            query += f" WHERE {where_clause}"

        # 添加ORDER BY
        if order_by:
            query += f" ORDER BY {order_by}"

        logger.info(f"开始读取MySQL表 {table_name} 的数据，批次大小: {batch_size}")

        try:
            # 使用pandas的chunksize进行分批读取
            for chunk in pd.read_sql(
                query,
                self.engine,
                chunksize=batch_size,
                index_col=None,
                parse_dates=True,  # 自动解析日期时间类型
                coerce_float=True  # 强制转换数值类型
            ):
                # 处理特殊值
                chunk = self._clean_dataframe(chunk)

                if len(chunk) > 0:
                    yield chunk

        except Exception as e:
            logger.error(f"读取MySQL数据失败: {e}")
            raise

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理DataFrame数据"""
        import numpy as np

        # 处理无穷大值
        float_cols = df.select_dtypes(include=[np.float64, np.float32]).columns
        for col in float_cols:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)

        return df

    def get_row_count(self, table_name: str,
                     where_clause: Optional[str] = None) -> int:
        """获取表行数"""
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"

        if where_clause:
            query += f" WHERE {where_clause}"

        with self.engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
            return result

    def get_min_max_value(self, table_name: str, column: str) -> tuple:
        """获取列的最小值和最大值，用于增量同步"""
        query = f"""
            SELECT MIN(`{column}`) as min_val,
                   MAX(`{column}`) as max_val
            FROM `{table_name}`
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            return result.min_val, result.max_val

    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取表的主键列"""
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :database
              AND TABLE_NAME = :table_name
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "database": self.database,
                "table_name": table_name
            }).fetchall()

            return [row.COLUMN_NAME for row in result]

    def execute_query(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        """执行自定义查询"""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})

            # 转换为字典列表
            columns = result.keys()
            rows = []
            for row in result:
                rows.append(dict(zip(columns, row)))

            return rows