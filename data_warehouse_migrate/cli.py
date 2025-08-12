"""
命令行接口模块
"""

import sys
import click
from typing import Optional

from .migrator import DataMigrator, MigrationMode
from .config import config
from .exceptions import DataWarehouseMigrateError, ConfigurationError
from .logger import setup_logger

logger = setup_logger(__name__)


@click.command()
@click.option('--source-project-id', 
              required=True,
              help='MaxCompute源项目ID')
@click.option('--source-table-name', 
              required=True,
              help='MaxCompute源表名')
@click.option('--destination-type',
              type=click.Choice(['bigquery', 'mysql'], case_sensitive=False),
              default='bigquery',
              help='目标数据源类型: bigquery 或 mysql')
@click.option('--destination-project-id', 
              help='BigQuery目标项目ID (仅当destination-type为bigquery时需要)')
@click.option('--destination-dataset-id', 
              help='BigQuery目标数据集ID (仅当destination-type为bigquery时需要)')
@click.option('--destination-table-name', 
              required=True,
              help='目标表名')
@click.option('--mode', 
              type=click.Choice(['overwrite', 'append'], case_sensitive=False),
              default='append',
              help='迁移模式: overwrite(覆盖) 或 append(追加)，默认为append')
@click.option('--batch-size',
              type=int,
              default=10000,
              help='批次大小，默认为10000')
@click.option('--maxcompute-access-id',
              help='MaxCompute AccessKey ID (可通过环境变量MAXCOMPUTE_ACCESS_ID设置)')
@click.option('--maxcompute-secret-key',
              help='MaxCompute AccessKey Secret (可通过环境变量MAXCOMPUTE_SECRET_ACCESS_KEY设置)')
@click.option('--maxcompute-endpoint',
              help='MaxCompute Endpoint (可通过环境变量MAXCOMPUTE_ENDPOINT设置)')
@click.option('--bigquery-credentials-path',
              help='BigQuery服务账号凭证文件路径 (可通过环境变量GOOGLE_APPLICATION_CREDENTIALS设置)')
@click.option('--mysql-dest-host',
              help='MySQL目标主机 (可通过环境变量MYSQL_DEST_HOST设置)')
@click.option('--mysql-dest-user',
              help='MySQL目标用户名 (可通过环境变量MYSQL_DEST_USER设置)')
@click.option('--mysql-dest-password',
              help='MySQL目标密码 (可通过环境变量MYSQL_DEST_PASSWORD设置)')
@click.option('--mysql-dest-database',
              help='MySQL目标数据库 (可通过环境变量MYSQL_DEST_DATABASE设置)')
@click.option('--mysql-dest-port',
              type=int,
              help='MySQL目标端口 (可通过环境变量MYSQL_DEST_PORT设置)')
@click.option('--log-level',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='INFO',
              help='日志级别，默认为INFO')
@click.option('--dry-run',
              is_flag=True,
              help='试运行模式，只检查连接和表结构，不实际迁移数据')
def main(source_project_id: str,
         source_table_name: str,
         destination_type: str,
         destination_project_id: Optional[str],
         destination_dataset_id: Optional[str],
         destination_table_name: str,
         mode: str,
         batch_size: int,
         maxcompute_access_id: Optional[str],
         maxcompute_secret_key: Optional[str],
         maxcompute_endpoint: Optional[str],
         bigquery_credentials_path: Optional[str],
         mysql_dest_host: Optional[str],
         mysql_dest_user: Optional[str],
         mysql_dest_password: Optional[str],
         mysql_dest_database: Optional[str],
         mysql_dest_port: Optional[int],
         log_level: str,
         dry_run: bool):
    """
    数据仓库迁移工具
    
    支持从阿里云MaxCompute迁移数据到Google Cloud BigQuery
    """
    
    # 设置日志级别
    logger.setLevel(getattr(__import__('logging'), log_level.upper()))
    
    try:
        # 显示配置信息
        click.echo("=" * 60)
        click.echo("数据仓库迁移工具")
        click.echo("=" * 60)
        click.echo(f"源项目ID: {source_project_id}")
        click.echo(f"源表名: {source_table_name}")
        click.echo(f"目标类型: {destination_type}")
        if destination_type == 'bigquery':
            click.echo(f"目标项目ID: {destination_project_id}")
            click.echo(f"目标数据集ID: {destination_dataset_id}")
        elif destination_type == 'mysql':
            click.echo(f"MySQL目标主机: {mysql_dest_host}")
            click.echo(f"MySQL目标数据库: {mysql_dest_database}")
        click.echo(f"目标表名: {destination_table_name}")
        click.echo(f"迁移模式: {mode}")
        click.echo(f"批次大小: {batch_size}")
        click.echo(f"日志级别: {log_level}")
        if dry_run:
            click.echo("模式: 试运行 (不会实际迁移数据)")
        click.echo("=" * 60)
        
        # 验证配置
        _validate_configuration(
            destination_type,
            maxcompute_access_id,
            maxcompute_secret_key,
            maxcompute_endpoint,
            bigquery_credentials_path,
            mysql_dest_host,
            mysql_dest_user,
            mysql_dest_password,
            mysql_dest_database,
            mysql_dest_port
        )
        
        # 创建迁移器
        migrator = DataMigrator(
            source_project_id=source_project_id,
            destination_type=destination_type,
            destination_project_id=destination_project_id, # For BigQuery
            maxcompute_access_id=maxcompute_access_id,
            maxcompute_secret_key=maxcompute_secret_key,
            maxcompute_endpoint=maxcompute_endpoint,
            bigquery_credentials_path=bigquery_credentials_path,
            mysql_dest_host=mysql_dest_host,
            mysql_dest_user=mysql_dest_user,
            mysql_dest_password=mysql_dest_password,
            mysql_dest_database=mysql_dest_database,
            mysql_dest_port=mysql_dest_port
        )
        
        # 转换迁移模式
        migration_mode = MigrationMode.OVERWRITE if mode.lower() == 'overwrite' else MigrationMode.APPEND
        
        if dry_run:
            # 试运行模式
            click.echo("开始试运行...")
            _dry_run(migrator, source_table_name, destination_table_name)
            click.echo("试运行完成！")
        else:
            # 实际迁移
            click.echo("开始数据迁移...")
            
            # 确认操作
            if mode.lower() == 'overwrite':
                if not click.confirm(f"警告：将覆盖目标表 {destination_table_name}，是否继续？"):
                    click.echo("操作已取消")
                    return
            
            migrator.migrate_table(
                source_table_name=source_table_name,
                destination_table_name=destination_table_name,
                mode=migration_mode,
                batch_size=batch_size,
                destination_dataset_id=destination_dataset_id,
                destination_database=mysql_dest_database
            )
            
            click.echo("数据迁移完成！")
        
    except DataWarehouseMigrateError as e:
        logger.error(f"迁移失败: {e}")
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"未知错误: {e}")
        click.echo(f"未知错误: {e}", err=True)
        sys.exit(1)


def _validate_configuration(destination_type: str,
                          maxcompute_access_id: Optional[str],
                          maxcompute_secret_key: Optional[str],
                          maxcompute_endpoint: Optional[str],
                          bigquery_credentials_path: Optional[str],
                          mysql_dest_host: Optional[str],
                          mysql_dest_user: Optional[str],
                          mysql_dest_password: Optional[str],
                          mysql_dest_database: Optional[str],
                          mysql_dest_port: Optional[int]) -> None:
    """验证配置"""
    
    # 检查MaxCompute配置
    mc_access_id = maxcompute_access_id or config.maxcompute_access_id
    mc_secret_key = maxcompute_secret_key or config.maxcompute_secret_access_key
    mc_endpoint = maxcompute_endpoint or config.maxcompute_endpoint
    
    if not all([mc_access_id, mc_secret_key, mc_endpoint]):
        raise ConfigurationError(
            "MaxCompute配置不完整，请设置以下参数或环境变量：\n"
            "- --maxcompute-access-id 或 MAXCOMPUTE_ACCESS_ID\n"
            "- --maxcompute-secret-key 或 MAXCOMPUTE_SECRET_ACCESS_KEY\n"
            "- --maxcompute-endpoint 或 MAXCOMPUTE_ENDPOINT"
        )
    
    # 检查目标配置
    if destination_type == 'bigquery':
        bq_credentials_path = bigquery_credentials_path or config.bigquery_credentials_path
        if not bq_credentials_path:
            logger.warning(
                "未设置BigQuery凭证文件路径，将尝试使用默认凭证。\n"
                "建议设置 --bigquery-credentials-path 或 GOOGLE_APPLICATION_CREDENTIALS 环境变量"
            )
    elif destination_type == 'mysql':
        mysql_host = mysql_dest_host or config.mysql_dest_host
        mysql_user = mysql_dest_user or config.mysql_dest_user
        mysql_password = mysql_dest_password or config.mysql_dest_password
        mysql_database = mysql_dest_database or config.mysql_dest_database
        
        if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
            raise ConfigurationError(
                "MySQL目标配置不完整，请设置以下参数或环境变量：\n"
                "- --mysql-dest-host 或 MYSQL_DEST_HOST\n"
                "- --mysql-dest-user 或 MYSQL_DEST_USER\n"
                "- --mysql-dest-password 或 MYSQL_DEST_PASSWORD\n"
                "- --mysql-dest-database 或 MYSQL_DEST_DATABASE"
            )


def _dry_run(migrator: DataMigrator, 
            source_table_name: str,
            destination_table_name: str) -> None:
    """试运行"""
    
    click.echo("1. 测试数据库连接...")
    migrator._test_connections()
    click.echo("   ✓ 连接测试通过")
    
    click.echo("2. 验证源表访问权限...")
    if migrator.maxcompute_client.validate_table_access(source_table_name):
        click.echo("   ✓ 源表访问验证成功")
    else:
        click.echo("   ✗ 源表访问验证失败")
        return

    click.echo("3. 获取源表结构...")
    columns = migrator.maxcompute_client.get_table_schema(source_table_name)
    partition_columns = [col for col in columns if col.get('is_partition', False)]
    regular_columns = [col for col in columns if not col.get('is_partition', False)]

    click.echo(f"   ✓ 源表包含 {len(regular_columns)} 个普通列")
    if partition_columns:
        partition_names = [col['name'] for col in partition_columns]
        click.echo(f"   ✓ 源表包含 {len(partition_columns)} 个分区字段: {', '.join(partition_names)}")

    click.echo("4. 转换表结构...")
    if migrator.destination_type == 'bigquery':
        destination_schema = migrator.schema_mapper.convert_maxcompute_to_bigquery_schema(columns)
        click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到BigQuery格式")
    elif migrator.destination_type == 'mysql':
        destination_schema = migrator.schema_mapper.convert_maxcompute_to_mysql_schema(columns)
        click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MySQL格式")
    
    click.echo("5. 检查目标表...")
    table_exists = migrator.destination_client.table_exists(destination_table_name)
    if table_exists:
        click.echo("   ✓ 目标表已存在")
    else:
        click.echo("   ✓ 目标表不存在，迁移时将创建")


if __name__ == '__main__':
    main()
