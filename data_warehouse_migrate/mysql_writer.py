import pandas as pd
from typing import Dict, Any, List
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus

class MySQLWriter:
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.engine = self._create_sqlalchemy_engine()

    def _create_sqlalchemy_engine(self):
        # URL-encode the password to handle special characters like '@', '#', etc.
        encoded_password = quote_plus(self.password)
        connection_string = f"mysql+mysqlconnector://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.database}"
        return create_engine(connection_string)

    def create_table(self, table_name: str, schema: List[Dict[str, Any]], mode: str):
        # 安全去重（按小写列名保留首次出现）
        deduped: List[Dict[str, Any]] = []
        seen_lower: set[str] = set()
        for col in schema:
            name = col['name']
            lower = name.lower()
            if lower in seen_lower:
                # 无日志对象，这里直接跳过；去重已在更上层记录
                continue
            seen_lower.add(lower)
            deduped.append(col)

        with self.engine.connect() as connection:
            columns_sql = []
            for col in deduped:
                col_name = col['name']
                col_type = col['type']
                columns_sql.append(f"`{col_name}` {col_type}")

            create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns_sql)})"
            connection.execute(text(create_table_sql))
            connection.commit()

    def write_dataframe(self, table_name: str, dataframe: pd.DataFrame, mode: str):
        # pandas.to_sql mode: 'append', 'replace', 'fail'
        # Our 'overwrite' mode maps to 'append' after truncation
        # Our 'append' mode maps to 'append'
        if_exists_mode = 'append'

        try:
            dataframe.to_sql(name=table_name, con=self.engine, if_exists=if_exists_mode, index=False)
        except Exception as e:
            raise RuntimeError(f"Failed to write DataFrame to MySQL table {table_name}: {e}")

    def table_exists(self, database: str, table_name: str) -> bool:
        with self.engine.connect() as connection:
            query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :database AND table_name = :table_name"
            result = connection.execute(text(query), {"database": database, "table_name": table_name}).scalar()
            return result > 0

    def truncate_table(self, table_name: str):
        with self.engine.connect() as connection:
            truncate_sql = f"TRUNCATE TABLE `{table_name}`"
            connection.execute(text(truncate_sql))
            connection.commit()

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as connection:
            query = f"""
                SELECT
                    COLUMN_NAME AS name,
                    DATA_TYPE AS type,
                    IS_NULLABLE AS is_nullable,
                    COLUMN_DEFAULT AS column_default
                FROM
                    information_schema.COLUMNS
                WHERE
                    TABLE_SCHEMA = :database AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION;
            """
            result = connection.execute(text(query), {
                "database": self.database,
                "table_name": table_name
            }).fetchall()

            schema = []
            for row in result:
                schema.append({
                    "name": row.name,
                    "type": row.type,
                    "is_nullable": row.is_nullable == 'YES',
                    "column_default": row.column_default
                })
            return schema

    def _test_connection(self):
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except OperationalError as e:
            raise ConnectionError(f"Failed to connect to MySQL: {e}")
