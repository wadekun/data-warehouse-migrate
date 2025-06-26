"""
自定义异常类
"""


class DataWarehouseMigrateError(Exception):
    """数据仓库迁移基础异常"""
    pass


class MaxComputeConnectionError(DataWarehouseMigrateError):
    """MaxCompute连接异常"""
    pass


class BigQueryConnectionError(DataWarehouseMigrateError):
    """BigQuery连接异常"""
    pass


class TableNotFoundError(DataWarehouseMigrateError):
    """表不存在异常"""
    pass


class SchemaConversionError(DataWarehouseMigrateError):
    """表结构转换异常"""
    pass


class DataMigrationError(DataWarehouseMigrateError):
    """数据迁移异常"""
    pass


class ConfigurationError(DataWarehouseMigrateError):
    """配置错误异常"""
    pass
