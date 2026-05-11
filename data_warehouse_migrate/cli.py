"""
命令行接口模块
"""

import sys
import click
from typing import Optional

from .migrator import DataMigrator, MigrationMode
from .config import config
from .exceptions import DataWarehouseMigrateError, ConfigurationError
from .config_loader import load_config_file, normalize_config, merge_with_cli_and_env, select_table_mapping
from .logger import setup_logger

logger = setup_logger(__name__)


@click.command()
@click.option('--source-type',
              type=click.Choice(['maxcompute', 'mysql'], case_sensitive=False),
              default=None,
              help='源数据源类型: maxcompute 或 mysql（可在配置文件中提供）')
@click.option('--source-project-id',
              help='MaxCompute源项目ID（当source-type为maxcompute时需要，可在配置文件中提供）')
@click.option('--source-table-name',
              help='源表名（可在配置文件中提供）')
@click.option('--destination-type',
              type=click.Choice(['bigquery', 'mysql', 'maxcompute'], case_sensitive=False),
              default=None,
              help='目标数据源类型: bigquery、mysql 或 maxcompute（可在配置文件中提供）')
@click.option('--destination-project-id', 
              help='BigQuery目标项目ID (仅当destination-type为bigquery时需要)')
@click.option('--destination-dataset-id', 
              help='BigQuery目标数据集ID (仅当destination-type为bigquery时需要)')
@click.option('--destination-table-name', 
              help='目标表名（可在配置文件中提供）')
@click.option('--mode', 
              type=click.Choice(['overwrite', 'append'], case_sensitive=False),
              default=None,
              help='迁移模式: overwrite(覆盖) 或 append(追加)（可在配置文件中提供）')
@click.option('--batch-size',
              type=int,
              default=None,
              help='批次大小（可在配置文件中提供）')
@click.option('--maxcompute-access-id',
              help='MaxCompute AccessKey ID (可通过环境变量MAXCOMPUTE_ACCESS_ID设置)')
@click.option('--maxcompute-secret-key',
              help='MaxCompute AccessKey Secret (可通过环境变量MAXCOMPUTE_SECRET_ACCESS_KEY设置)')
@click.option('--maxcompute-endpoint',
              help='MaxCompute Endpoint (可通过环境变量MAXCOMPUTE_ENDPOINT设置)')
# MySQL源配置
@click.option('--mysql-source-host',
              help='MySQL源主机 (当source-type为mysql时需要)')
@click.option('--mysql-source-user',
              help='MySQL源用户名')
@click.option('--mysql-source-password',
              help='MySQL源密码')
@click.option('--mysql-source-database',
              help='MySQL源数据库名')
@click.option('--mysql-source-port',
              type=int,
              help='MySQL源端口')
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
              default=None,
              help='日志级别（可在配置文件中提供）')
@click.option('--preserve-string-null-tokens/--no-preserve-string-null-tokens',
              default=None,
              help='是否在字符串列中保留字面量空值标记（NULL/None/NaN等）。默认取环境变量配置，未设置时为保留')
@click.option('--string-null-tokens',
              default=None,
              help='逗号分隔的字符串空值标记列表，例如: "nan,None,null,<NA>,NaN"')
@click.option('--null-on-non-nullable',
              type=click.Choice(['fail', 'fill', 'skip'], case_sensitive=False),
              default=None,
              help='当写入非空列遇到NULL时的策略：fail（报错）、fill（填充哨兵值）、skip（跳过含NULL的行）')
@click.option('--null-fill-sentinel',
              default=None,
              help='当策略为fill时用于填充字符串/日期列的哨兵值，如 "N/A" 或 空字符串')
@click.option('--partition-filter',
              default=None,
              help='MaxCompute分区过滤条件，如: "pt=\'20240501\'" 或 '
                   '"pt>=\'20240401\' AND pt<=\'20240501\'"。'
                   '设为 "*" 查询所有分区。仅当源为MaxCompute分区表时生效。')
@click.option('--dry-run',
              is_flag=True,
              help='试运行模式，只检查连接和表结构，不实际迁移数据（适用于所有数据流方向）')
@click.option('-f', '--config-file',
              default=None,
              help='配置文件路径（JSON），CLI > 文件 > 环境 进行合并')
def main(source_type: str,
         source_project_id: str,
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
         mysql_source_host: Optional[str],
         mysql_source_user: Optional[str],
         mysql_source_password: Optional[str],
         mysql_source_database: Optional[str],
         mysql_source_port: Optional[int],
         bigquery_credentials_path: Optional[str],
         mysql_dest_host: Optional[str],
         mysql_dest_user: Optional[str],
         mysql_dest_password: Optional[str],
         mysql_dest_database: Optional[str],
         mysql_dest_port: Optional[int],
         preserve_string_null_tokens: Optional[bool],
         string_null_tokens: Optional[str],
         null_on_non_nullable: Optional[str],
         null_fill_sentinel: Optional[str],
         partition_filter: Optional[str],
         log_level: str,
         dry_run: bool,
         config_file: Optional[str]):
    """
    数据仓库迁移工具

    支持以下数据流方向：
    1. MaxCompute -> BigQuery
    2. MaxCompute -> MySQL
    3. MySQL -> MaxCompute
    """
    
    # 初始日志级别（可能为None）；最终会在参数合并后再次设置
    if log_level:
        logger.setLevel(getattr(__import__('logging'), str(log_level).upper()))
    
    try:
        # 1) 从配置文件读取并规范化
        file_cfg = {}
        if config_file:
            file_cfg = normalize_config(load_config_file(config_file))

        # 2) 将 CLI 实参打包为 dict 参与合并
        cli_args = {
            "source_type": source_type,
            "source_project_id": source_project_id,
            "source_table_name": source_table_name,
            "destination_type": destination_type,
            "destination_project_id": destination_project_id,
            "destination_dataset_id": destination_dataset_id,
            "destination_table_name": destination_table_name,
            "mode": mode,
            "batch_size": batch_size,
            "maxcompute_access_id": maxcompute_access_id,
            "maxcompute_secret_key": maxcompute_secret_key,
            "maxcompute_endpoint": maxcompute_endpoint,
            # MySQL源配置
            "mysql_source_host": mysql_source_host,
            "mysql_source_user": mysql_source_user,
            "mysql_source_password": mysql_source_password,
            "mysql_source_database": mysql_source_database,
            "mysql_source_port": mysql_source_port,
            "bigquery_credentials_path": bigquery_credentials_path,
            # MySQL目标配置
            "mysql_dest_host": mysql_dest_host,
            "mysql_dest_user": mysql_dest_user,
            "mysql_dest_password": mysql_dest_password,
            "mysql_dest_database": mysql_dest_database,
            "mysql_dest_port": mysql_dest_port,
            "preserve_string_null_tokens": preserve_string_null_tokens,
            "string_null_tokens": string_null_tokens,
            "null_on_non_nullable": null_on_non_nullable,
            "null_fill_sentinel": null_fill_sentinel,
            "partition_filter": partition_filter,
            "log_level": log_level,
            "dry_run": dry_run,
        }

        # 3) 合并：CLI > 文件 > 环境
        final_args = merge_with_cli_and_env(cli_args, file_cfg, config)

        # 将关键生效参数回写到局部变量，便于后续沿用
        source_type = (final_args.get("source_type") or "").lower() or "maxcompute"
        source_project_id = final_args.get("source_project_id")
        source_table_name = final_args.get("source_table_name")
        destination_type = (final_args.get("destination_type") or "").lower() or "bigquery"
        destination_project_id = final_args.get("destination_project_id")
        destination_dataset_id = final_args.get("destination_dataset_id")
        destination_table_name = final_args.get("destination_table_name")
        mode = final_args.get("mode", "append")
        batch_size = final_args.get("batch_size", 10000)
        maxcompute_access_id = final_args.get("maxcompute_access_id")
        maxcompute_secret_key = final_args.get("maxcompute_secret_key")
        maxcompute_endpoint = final_args.get("maxcompute_endpoint")
        # MySQL源参数
        mysql_source_host = final_args.get("mysql_source_host")
        mysql_source_user = final_args.get("mysql_source_user")
        mysql_source_password = final_args.get("mysql_source_password")
        mysql_source_database = final_args.get("mysql_source_database")
        mysql_source_port = final_args.get("mysql_source_port")
        # MySQL目标参数
        mysql_dest_host = final_args.get("mysql_dest_host")
        mysql_dest_user = final_args.get("mysql_dest_user")
        mysql_dest_password = final_args.get("mysql_dest_password")
        mysql_dest_database = final_args.get("mysql_dest_database")
        mysql_dest_port = final_args.get("mysql_dest_port")
        bigquery_credentials_path = final_args.get("bigquery_credentials_path")
        preserve_tokens = final_args.get("preserve_string_null_tokens", config.preserve_string_null_tokens)
        tokens = final_args.get("string_null_tokens", config.string_null_tokens)
        null_policy = final_args.get("null_on_non_nullable", config.null_on_non_nullable)
        null_sentinel = final_args.get("null_fill_sentinel", config.null_fill_sentinel)
        partition_filter = final_args.get("partition_filter", partition_filter)
        log_level = final_args.get("log_level", log_level)
        dry_run = final_args.get("dry_run", dry_run)

        # 选择字段映射（仅 MySQL 的一期生效）
        active_mapping = None
        if destination_type == 'mysql' and config_file:
            # 直接从原始文件加载（未扁平化）以读取 mappings 段
            raw_cfg = load_config_file(config_file)
            active_mapping = select_table_mapping(raw_cfg, source_table_name)

        # 使用最终参数打印信息
        click.echo("=" * 60)
        click.echo("数据仓库迁移工具")
        click.echo("=" * 60)
        click.echo(f"源类型: {source_type}")
        if source_type == 'maxcompute':
            click.echo(f"源项目ID: {source_project_id}")
        elif source_type == 'mysql':
            click.echo(f"MySQL源主机: {mysql_source_host}")
            click.echo(f"MySQL源数据库: {mysql_source_database}")
        click.echo(f"源表名: {source_table_name}")
        click.echo(f"目标类型: {destination_type}")
        if destination_type == 'bigquery':
            click.echo(f"目标项目ID: {destination_project_id}")
            click.echo(f"目标数据集ID: {destination_dataset_id}")
        elif destination_type == 'mysql':
            click.echo(f"MySQL目标主机: {mysql_dest_host}")
            click.echo(f"MySQL目标数据库: {mysql_dest_database}")
        elif destination_type == 'maxcompute':
            click.echo(f"目标项目ID: {destination_project_id}")
        click.echo(f"目标表名: {destination_table_name}")
        click.echo(f"迁移模式: {mode}")
        click.echo(f"批次大小: {batch_size}")
        if source_type == 'maxcompute' and partition_filter:
            click.echo(f"分区条件: {partition_filter}")
        click.echo(f"日志级别: {log_level}")
        if dry_run:
            click.echo("模式: 试运行 (不会实际迁移数据)")
        click.echo("=" * 60)

        # 以最终日志级别更新logger等级
        logger.setLevel(getattr(__import__('logging'), str(log_level).upper()))

        # 终态参数校验
        _validate_configuration(
            source_type=source_type,
            destination_type=destination_type,
            maxcompute_access_id=maxcompute_access_id,
            maxcompute_secret_key=maxcompute_secret_key,
            maxcompute_endpoint=maxcompute_endpoint,
            bigquery_credentials_path=bigquery_credentials_path,
            # MySQL源配置
            mysql_source_host=mysql_source_host,
            mysql_source_user=mysql_source_user,
            mysql_source_password=mysql_source_password,
            mysql_source_database=mysql_source_database,
            # MySQL目标配置
            mysql_dest_host=mysql_dest_host,
            mysql_dest_user=mysql_dest_user,
            mysql_dest_password=mysql_dest_password,
            mysql_dest_database=mysql_dest_database,
            mysql_dest_port=mysql_dest_port
        )

        # 创建迁移器
        migrator = DataMigrator(
            source_type=source_type,
            source_project_id=source_project_id,
            source_table_name=source_table_name,
            destination_type=destination_type,
            destination_project_id=destination_project_id, # For BigQuery or MaxCompute
            maxcompute_access_id=maxcompute_access_id,
            maxcompute_secret_key=maxcompute_secret_key,
            maxcompute_endpoint=maxcompute_endpoint,
            # MySQL源配置
            mysql_source_host=mysql_source_host,
            mysql_source_user=mysql_source_user,
            mysql_source_password=mysql_source_password,
            mysql_source_database=mysql_source_database,
            mysql_source_port=mysql_source_port,
            bigquery_credentials_path=bigquery_credentials_path,
            # MySQL目标配置
            mysql_dest_host=mysql_dest_host,
            mysql_dest_user=mysql_dest_user,
            mysql_dest_password=mysql_dest_password,
            mysql_dest_database=mysql_dest_database,
            mysql_dest_port=mysql_dest_port,
            preserve_string_null_tokens=preserve_tokens,
            string_null_tokens=tokens,
            null_on_non_nullable=null_policy,
            null_fill_sentinel=null_sentinel,
            column_mapping_plan=active_mapping if destination_type == 'mysql' else None,
            partition_filter=partition_filter if source_type == 'maxcompute' else None
        )
        
        # 转换迁移模式
        migration_mode = MigrationMode.OVERWRITE if mode.lower() == 'overwrite' else MigrationMode.APPEND
        
        if dry_run:
            # 试运行模式
            click.echo("开始试运行...")
            _dry_run(migrator, source_table_name, destination_table_name, destination_dataset_id)
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


def _validate_configuration(source_type: str,
                          destination_type: str,
                          maxcompute_access_id: Optional[str],
                          maxcompute_secret_key: Optional[str],
                          maxcompute_endpoint: Optional[str],
                          bigquery_credentials_path: Optional[str],
                          # MySQL源配置
                          mysql_source_host: Optional[str] = None,
                          mysql_source_user: Optional[str] = None,
                          mysql_source_password: Optional[str] = None,
                          mysql_source_database: Optional[str] = None,
                          # MySQL目标配置
                          mysql_dest_host: Optional[str] = None,
                          mysql_dest_user: Optional[str] = None,
                          mysql_dest_password: Optional[str] = None,
                          mysql_dest_database: Optional[str] = None,
                          mysql_dest_port: Optional[int] = None) -> None:
    """验证配置"""
    
    # 根据数据流方向验证配置
    # 如果源或目标涉及MaxCompute，需要验证MaxCompute配置
    if source_type == 'maxcompute' or destination_type == 'maxcompute':
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

    # 如果源是MySQL，需要验证MySQL源配置
    if source_type == 'mysql':
        if not all([mysql_source_host, mysql_source_user, mysql_source_password, mysql_source_database]):
            raise ConfigurationError(
                "MySQL源配置不完整，请设置以下参数：\n"
                "- --mysql-source-host\n"
                "- --mysql-source-user\n"
                "- --mysql-source-password\n"
                "- --mysql-source-database"
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
            destination_table_name: str,
            destination_dataset_id: Optional[str] = None) -> None:
    """试运行"""

    click.echo("1. 测试数据库连接...")
    migrator._test_connections()
    click.echo("   ✓ 连接测试通过")

    click.echo("2. 验证源表访问权限...")
    if migrator.source_type == 'maxcompute' and migrator.partition_filter:
        click.echo(f"   分区过滤条件: {migrator.partition_filter}")
    elif migrator.source_type == 'maxcompute':
        click.echo("   分区策略: 自动选择最新分区")
    # 根据源类型进行不同的验证
    if migrator.source_type == 'maxcompute':
        if hasattr(migrator.source_client, 'validate_table_access'):
            pf = migrator.partition_filter if migrator.source_type == 'maxcompute' else None
            if migrator.source_client.validate_table_access(source_table_name,
                                                            partition_filter=pf):
                click.echo("   ✓ MaxCompute源表访问验证成功")
            else:
                click.echo("   ✗ MaxCompute源表访问验证失败")
                return
        else:
            # 尝试获取表结构来验证访问权限
            try:
                migrator.source_client.get_table_schema(source_table_name)
                click.echo("   ✓ MaxCompute源表访问验证成功")
            except Exception as e:
                click.echo(f"   ✗ MaxCompute源表访问验证失败: {e}")
                return
    else:
        # MySQL或其他源类型，尝试获取表结构来验证
        try:
            migrator.source_client.get_table_schema(source_table_name)
            click.echo(f"   ✓ {migrator.source_type.title()}源表访问验证成功")
        except Exception as e:
            click.echo(f"   ✗ {migrator.source_type.title()}源表访问验证失败: {e}")
            return

    click.echo("3. 获取源表结构...")
    columns = migrator.source_client.get_table_schema(source_table_name)

    # 根据源类型处理分区信息
    if migrator.source_type == 'maxcompute':
        partition_columns = [col for col in columns if col.get('is_partition', False)]
        regular_columns = [col for col in columns if not col.get('is_partition', False)]
        click.echo(f"   ✓ 源表包含 {len(regular_columns)} 个普通列")
        if partition_columns:
            partition_names = [col['name'] for col in partition_columns]
            click.echo(f"   ✓ 源表包含 {len(partition_columns)} 个分区字段: {', '.join(partition_names)}")
    else:
        # MySQL表没有分区概念
        click.echo(f"   ✓ {migrator.source_type.title()}源表包含 {len(columns)} 个列")

        # 显示列信息预览（仅前10列）
        if len(columns) > 0:
            click.echo("   主要列信息:")
            for i, col in enumerate(columns[:10]):
                nullable = "可空" if col.get('is_nullable', True) else "非空"
                comment = col.get('comment', '')
                comment = f" - {comment}" if comment else ""
                click.echo(f"     - {col['name']}: {col['type']} ({nullable}){comment}")
            if len(columns) > 10:
                click.echo(f"     ... 还有 {len(columns) - 10} 列")

    click.echo("4. 转换表结构...")
    # 根据数据流方向进行不同的转换
    if migrator.source_type == 'maxcompute' and migrator.destination_type == 'bigquery':
        destination_schema = migrator.schema_mapper.convert_maxcompute_to_bigquery_schema(columns)
        click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到BigQuery格式")
    elif migrator.source_type == 'maxcompute' and migrator.destination_type == 'mysql':
        # 应用字段映射计划（仅 MySQL 目的一期生效）
        if getattr(migrator, 'column_mapping_plan', None):
            try:
                summary = migrator.generate_mysql_mapping_summary(columns)
                if summary and summary.get('final_columns'):
                    # 使用映射后的列信息
                    prepared_columns, overrides, _ = migrator._prepare_mysql_schema_inputs(
                        columns, migrator.column_mapping_plan
                    )
                    destination_schema = migrator.schema_mapper.convert_maxcompute_to_mysql_schema(
                        prepared_columns,
                        overrides=overrides
                    )
                    click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MySQL格式（应用映射）")
                else:
                    destination_schema = migrator.schema_mapper.convert_maxcompute_to_mysql_schema(columns)
                    click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MySQL格式")
            except Exception as e:
                logger.debug(f"dry-run MySQL映射转换失败，使用基础转换: {e}")
                destination_schema = migrator.schema_mapper.convert_maxcompute_to_mysql_schema(columns)
                click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MySQL格式")
        else:
            destination_schema = migrator.schema_mapper.convert_maxcompute_to_mysql_schema(columns)
            click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MySQL格式")
    elif migrator.source_type == 'mysql' and migrator.destination_type == 'maxcompute':
        destination_schema = migrator.schema_mapper.convert_mysql_to_maxcompute_schema(columns)
        click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到MaxCompute格式")
    elif migrator.source_type == 'mysql' and migrator.destination_type == 'bigquery':
        # MySQL到BigQuery需要两步转换：MySQL -> MaxCompute -> BigQuery
        temp_schema = migrator.schema_mapper.convert_mysql_to_maxcompute_schema(columns)
        destination_schema = migrator.schema_mapper.convert_maxcompute_to_bigquery_schema(temp_schema)
        click.echo(f"   ✓ 成功转换 {len(destination_schema)} 列到BigQuery格式")
    else:
        click.echo(f"   ✓ 支持的数据流: {migrator.source_type} -> {migrator.destination_type}")
    
    click.echo("5. 检查目标表...")
    if migrator.destination_type == 'mysql':
        table_exists = migrator.destination_client.table_exists(
            migrator.destination_client.database,
            destination_table_name
        )
    elif migrator.destination_type == 'bigquery':
        if not destination_dataset_id:
            click.echo("   ✗ 未提供BigQuery目标数据集ID，无法检查目标表")
            return
        table_exists = migrator.destination_client.table_exists(
            destination_dataset_id,
            destination_table_name
        )
    elif migrator.destination_type == 'maxcompute':
        table_exists = migrator.destination_client.table_exists(destination_table_name)
    else:
        click.echo(f"   ⚠ 不支持的目标类型: {migrator.destination_type}")
        table_exists = False

    if table_exists:
        click.echo("   ✓ 目标表已存在")
    else:
        click.echo("   ✓ 目标表不存在，迁移时将创建")

    # 6. 获取源表行数（可选）
    try:
        if hasattr(migrator.source_client, 'get_row_count'):
            row_count = migrator.source_client.get_row_count(source_table_name)
            click.echo(f"6. 源表数据量...")
            click.echo(f"   ✓ 预计行数: {row_count:,}")
    except Exception as e:
        logger.debug(f"dry-run 获取行数失败: {e}")

    # 7. 映射摘要（仅 MySQL 目标且存在映射）
    if migrator.destination_type == 'mysql' and getattr(migrator, 'column_mapping_plan', None):
        try:
            summary = migrator.generate_mysql_mapping_summary(columns)
            if summary:
                click.echo("6. 映射摘要（MySQL）...")
                if summary.get('include'):
                    click.echo(f"   include: {', '.join(summary['include'])}")
                if summary.get('exclude'):
                    click.echo(f"   exclude: {', '.join(summary['exclude'])}")
                if summary.get('rename'):
                    pairs = [f"{k} -> {v}" for k, v in summary['rename'].items()]
                    click.echo(f"   rename: {', '.join(pairs)}")
                if summary.get('computed'):
                    click.echo(f"   computed: {', '.join(summary['computed'])}")
                if summary.get('type_override'):
                    pairs = [f"{k}: {v}" for k, v in summary['type_override'].items()]
                    click.echo(f"   type_override: {', '.join(pairs)}")
                if summary.get('order'):
                    click.echo(f"   order: {', '.join(summary['order'])}")
                click.echo(f"   最终目标列数: {summary.get('final_count')}")
                finals = summary.get('final_columns') or []
                if finals:
                    preview = finals if len(finals) <= 30 else finals[:30] + ['...']
                    click.echo(f"   最终目标列: {', '.join(preview)}")
        except Exception as e:
            logger.debug(f"dry-run 映射摘要输出失败: {e}")


if __name__ == '__main__':
    main()
