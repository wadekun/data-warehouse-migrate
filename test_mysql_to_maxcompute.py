#!/usr/bin/env python3
"""
测试MySQL到MaxCompute的同步功能
"""

import os
import sys
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.migrator import DataMigrator, MigrationMode
from data_warehouse_migrate.logger import setup_logger

# 设置日志
logger = setup_logger(__name__)
logger.setLevel(logging.INFO)


def test_mysql_to_maxcompute():
    """测试MySQL到MaxCompute的同步"""

    # 测试配置
    # 这些参数需要根据实际环境进行修改
    config = {
        # 基础配置
        'source_type': 'mysql',
        'source_table_name': 'test_table',
        'destination_type': 'maxcompute',
        'destination_table_name': 'test_table_from_mysql',
        'mode': 'overwrite',
        'batch_size': 1000,

        # MySQL源配置（请根据实际情况修改）
        'mysql_source_host': 'localhost',
        'mysql_source_port': 3306,
        'mysql_source_user': 'root',
        'mysql_source_password': 'your_password',
        'mysql_source_database': 'test_db',

        # MaxCompute目标配置（请根据实际情况修改）
        'maxcompute_dest_project_id': 'your_project',
        'maxcompute_access_id': 'your_access_id',
        'maxcompute_secret_key': 'your_secret_key',
        'maxcompute_endpoint': 'http://service.cn.maxcompute.aliyun.com/api',

        # 兼容性配置
        'preserve_string_null_tokens': True,
        'string_null_tokens': ['NULL', 'null', ''],
        'null_on_non_nullable': 'fill',
        'null_fill_sentinel': ''
    }

    try:
        logger.info("=" * 60)
        logger.info("开始测试MySQL到MaxCompute同步")
        logger.info("=" * 60)

        # 创建迁移器
        migrator = DataMigrator(
            source_type=config['source_type'],
            source_table_name=config['source_table_name'],
            destination_type=config['destination_type'],
            destination_project_id=config['maxcompute_dest_project_id'],
            maxcompute_access_id=config['maxcompute_access_id'],
            maxcompute_secret_key=config['maxcompute_secret_key'],
            maxcompute_endpoint=config['maxcompute_endpoint'],
            mysql_source_host=config['mysql_source_host'],
            mysql_source_user=config['mysql_source_user'],
            mysql_source_password=config['mysql_source_password'],
            mysql_source_database=config['mysql_source_database'],
            mysql_source_port=config['mysql_source_port'],
            preserve_string_null_tokens=config['preserve_string_null_tokens'],
            string_null_tokens=config['string_null_tokens'],
            null_on_non_nullable=config['null_on_non_nullable'],
            null_fill_sentinel=config['null_fill_sentinel']
        )

        # 测试连接
        logger.info("测试连接...")
        if not migrator.source_client.test_connection():
            logger.error("MySQL源连接失败")
            return False

        if not migrator.destination_client.test_connection():
            logger.error("MaxCompute目标连接失败")
            return False

        logger.info("连接测试成功！")

        # 获取源表结构
        logger.info(f"获取源表 {config['source_table_name']} 的结构...")
        source_schema = migrator.source_client.get_table_schema(config['source_table_name'])
        logger.info(f"源表共有 {len(source_schema)} 列")

        # 转换为目标结构
        from data_warehouse_migrate.schema_mapper import SchemaMapper
        destination_schema = SchemaMapper.convert_mysql_to_maxcompute_schema(source_schema)
        logger.info(f"目标表共有 {len(destination_schema)} 列")

        # 创建测试数据表（如果需要）
        logger.info("准备测试数据...")

        # 执行迁移
        logger.info("开始迁移数据...")
        migrator.migrate_table(
            source_table_name=config['source_table_name'],
            destination_table_name=config['destination_table_name'],
            mode=MigrationMode.OVERWRITE,
            batch_size=config['batch_size']
        )

        logger.info("数据迁移完成！")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"测试失败: {e}", exc_info=True)
        return False


def test_cli_command():
    """测试CLI命令"""

    logger.info("\n" + "=" * 60)
    logger.info("测试CLI命令示例")
    logger.info("=" * 60)

    # MySQL到MaxCompute的CLI命令示例
    cli_example = """
    data-warehouse-migrate \\
      --source-type mysql \\
      --source-table-name your_table \\
      --mysql-source-host localhost \\
      --mysql-source-user root \\
      --mysql-source-password your_password \\
      --mysql-source-database your_db \\
      --destination-type maxcompute \\
      --destination-project-id your_maxcompute_project \\
      --destination-table-name your_table_from_mysql \\
      --maxcompute-access-id your_access_id \\
      --maxcompute-secret-key your_secret_key \\
      --maxcompute-endpoint http://service.cn.maxcompute.aliyun.com/api \\
      --mode overwrite \\
      --batch-size 10000 \\
      --dry-run
    """

    logger.info("CLI命令示例:")
    logger.info(cli_example)

    # 配置文件示例
    config_example = """
    {
      "source": {
        "type": "mysql",
        "mysql": {
          "host": "localhost",
          "port": 3306,
          "user": "root",
          "password": "${MYSQL_PASSWORD}",
          "database": "production_db"
        },
        "table_name": "orders"
      },
      "destination": {
        "type": "maxcompute",
        "maxcompute": {
          "project_id": "data_warehouse",
          "access_id": "${MAXCOMPUTE_ACCESS_ID}",
          "secret_key": "${MAXCOMPUTE_SECRET_KEY}",
          "endpoint": "http://service.cn.maxcompute.aliyun.com/api"
        },
        "table_name": "ods_orders"
      },
      "run": {
        "mode": "append",
        "batch_size": 50000,
        "log_level": "INFO"
      }
    }
    """

    logger.info("\n配置文件示例 (mysql-to-maxcompute.json):")
    logger.info(config_example)

    logger.info("\n使用配置文件的命令:")
    logger.info("data-warehouse-migrate -f mysql-to-maxcompute.json")


if __name__ == "__main__":
    # 运行测试
    # success = test_mysql_to_maxcompute()

    # 打印CLI使用示例
    test_cli_command()

    logger.info("\n注意事项:")
    logger.info("1. 请根据实际环境修改配置参数")
    logger.info("2. 确保MySQL用户有读取权限")
    logger.info("3. 确保MaxCompute项目有创建表和写入数据的权限")
    logger.info("4. 大数据量迁移建议使用较小的批次大小")
    logger.info("5. 可以使用 --dry-run 参数先进行试运行")