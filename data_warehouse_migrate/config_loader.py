"""
配置文件加载与规范化工具

阶段一：仅支持 JSON 配置文件；提供扁平化与分组键规范化、环境变量占位符展开、
以及与 CLI/环境配置的合并工具。
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict


def _expand_env(value: Any) -> Any:
    """
    对字符串执行环境变量占位符展开（支持 $VAR 与 ${VAR}），递归处理 dict/list。
    其他类型原样返回。
    """
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _to_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _to_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(str(val))
    except Exception:
        return None


def _to_list(val: Any) -> list | None:
    if val is None:
        return None
    if isinstance(val, list):
        return val
    # 逗号分隔字符串
    if isinstance(val, str):
        parts = [p.strip() for p in val.split(",")]
        return [p for p in parts if p]
    return None


def load_config_file(path: str) -> Dict[str, Any]:
    """
    加载 JSON 配置文件并展开环境变量占位符。
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return _expand_env(raw)


def normalize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    将分组键规范化为扁平键；保留已是扁平键的字段。
    支持的分组：source、destination、destination.mysql、run、compat。
    """
    out: Dict[str, Any] = {}

    # 已扁平键优先直接放入
    out.update({k: v for k, v in cfg.items() if not isinstance(v, dict)})

    # source
    source = cfg.get("source") or {}
    if isinstance(source, dict):
        if "project_id" in source:
            out.setdefault("source_project_id", source.get("project_id"))
        if "table_name" in source:
            out.setdefault("source_table_name", source.get("table_name"))
        # MaxCompute 认证
        if "maxcompute_access_id" in source:
            out.setdefault("maxcompute_access_id", source.get("maxcompute_access_id"))
        if "maxcompute_secret_key" in source:
            out.setdefault("maxcompute_secret_key", source.get("maxcompute_secret_key"))
        if "maxcompute_endpoint" in source:
            out.setdefault("maxcompute_endpoint", source.get("maxcompute_endpoint"))

    # destination
    dest = cfg.get("destination") or {}
    if isinstance(dest, dict):
        if "type" in dest:
            out.setdefault("destination_type", dest.get("type"))
        if "table_name" in dest:
            out.setdefault("destination_table_name", dest.get("table_name"))
        if "project_id" in dest:
            out.setdefault("destination_project_id", dest.get("project_id"))
        if "dataset_id" in dest:
            out.setdefault("destination_dataset_id", dest.get("dataset_id"))
        # destination.mysql
        dmysql = dest.get("mysql") or {}
        if isinstance(dmysql, dict):
            if "host" in dmysql:
                out.setdefault("mysql_dest_host", dmysql.get("host"))
            if "port" in dmysql:
                out.setdefault("mysql_dest_port", dmysql.get("port"))
            if "user" in dmysql:
                out.setdefault("mysql_dest_user", dmysql.get("user"))
            if "password" in dmysql:
                out.setdefault("mysql_dest_password", dmysql.get("password"))
            if "database" in dmysql:
                out.setdefault("mysql_dest_database", dmysql.get("database"))

    # run
    run = cfg.get("run") or {}
    if isinstance(run, dict):
        for key in ["mode", "batch_size", "log_level", "dry_run"]:
            if key in run:
                out.setdefault(key, run.get(key))

    # compat
    compat = cfg.get("compat") or {}
    if isinstance(compat, dict):
        for key in [
            "preserve_string_null_tokens",
            "string_null_tokens",
            "null_on_non_nullable",
            "null_fill_sentinel",
            "string_null_tokens_case_insensitive",
            "treat_empty_string_as_null",
        ]:
            if key in compat:
                out.setdefault(key, compat.get(key))

    # 类型规整
    # 端口/批次
    port = _to_int(out.get("mysql_dest_port"))
    if port is not None:
        out["mysql_dest_port"] = port
    batch = _to_int(out.get("batch_size"))
    if batch is not None:
        out["batch_size"] = batch

    # 布尔
    for bkey in [
        "dry_run",
        "preserve_string_null_tokens",
        "string_null_tokens_case_insensitive",
        "treat_empty_string_as_null",
    ]:
        bval = _to_bool(out.get(bkey))
        if bval is not None:
            out[bkey] = bval

    # 列表
    lst = _to_list(out.get("string_null_tokens"))
    if lst is not None:
        out["string_null_tokens"] = lst

    return out


def merge_with_cli_and_env(cli_args: Dict[str, Any], file_cfg: Dict[str, Any], env_cfg) -> Dict[str, Any]:
    """
    合并参数：CLI > 文件 > 环境（config.py 的 config）。
    仅对本项目使用到的键进行合并。
    """
    merged: Dict[str, Any] = {}

    def pick(key: str, cli_val: Any, file_key: str | None = None, env_val: Any | None = None):
        fk = file_key or key
        if cli_val is not None:
            merged[key] = cli_val
        elif fk in file_cfg and file_cfg.get(fk) is not None:
            merged[key] = file_cfg.get(fk)
        else:
            if env_val is not None:
                merged[key] = env_val

    # 基本参数
    pick("source_project_id", cli_args.get("source_project_id"), env_val=None)
    pick("source_table_name", cli_args.get("source_table_name"), env_val=None)

    pick("destination_type", cli_args.get("destination_type"), env_val=None)
    pick("destination_project_id", cli_args.get("destination_project_id"), env_val=None)
    pick("destination_dataset_id", cli_args.get("destination_dataset_id"), env_val=None)
    pick("destination_table_name", cli_args.get("destination_table_name"), env_val=None)

    # MaxCompute
    pick("maxcompute_access_id", cli_args.get("maxcompute_access_id"), env_val=env_cfg.maxcompute_access_id)
    pick("maxcompute_secret_key", cli_args.get("maxcompute_secret_key"), env_val=env_cfg.maxcompute_secret_access_key)
    pick("maxcompute_endpoint", cli_args.get("maxcompute_endpoint"), env_val=env_cfg.maxcompute_endpoint)

    # BigQuery
    pick("bigquery_credentials_path", cli_args.get("bigquery_credentials_path"), env_val=env_cfg.bigquery_credentials_path)

    # MySQL
    pick("mysql_dest_host", cli_args.get("mysql_dest_host"), env_val=env_cfg.mysql_dest_host)
    pick("mysql_dest_user", cli_args.get("mysql_dest_user"), env_val=env_cfg.mysql_dest_user)
    pick("mysql_dest_password", cli_args.get("mysql_dest_password"), env_val=env_cfg.mysql_dest_password)
    pick("mysql_dest_database", cli_args.get("mysql_dest_database"), env_val=env_cfg.mysql_dest_database)
    pick("mysql_dest_port", cli_args.get("mysql_dest_port"), env_val=env_cfg.mysql_dest_port)

    # 运行参数
    pick("mode", cli_args.get("mode"), env_val=None)
    pick("batch_size", cli_args.get("batch_size"), env_val=None)
    pick("log_level", cli_args.get("log_level"), env_val=env_cfg.log_level)
    pick("dry_run", cli_args.get("dry_run"), env_val=False)

    # 兼容性/清洗策略
    pick("preserve_string_null_tokens", cli_args.get("preserve_string_null_tokens"), env_val=env_cfg.preserve_string_null_tokens)
    pick("string_null_tokens", cli_args.get("string_null_tokens"), env_val=env_cfg.string_null_tokens)
    pick("null_on_non_nullable", cli_args.get("null_on_non_nullable"), env_val=env_cfg.null_on_non_nullable)
    pick("null_fill_sentinel", cli_args.get("null_fill_sentinel"), env_val=env_cfg.null_fill_sentinel)

    # 应用文件覆盖（在 CLI 与 env 之后补齐缺省）
    for k, v in file_cfg.items():
        if merged.get(k) is None and v is not None:
            merged[k] = v

    # 类型兜底：端口/批次/布尔/列表
    if isinstance(merged.get("mysql_dest_port"), str):
        iv = _to_int(merged.get("mysql_dest_port"))
        if iv is not None:
            merged["mysql_dest_port"] = iv
    if isinstance(merged.get("batch_size"), str):
        iv = _to_int(merged.get("batch_size"))
        if iv is not None:
            merged["batch_size"] = iv
    for bkey in ["dry_run", "preserve_string_null_tokens"]:
        if isinstance(merged.get(bkey), str):
            bv = _to_bool(merged.get(bkey))
            if bv is not None:
                merged[bkey] = bv
    if isinstance(merged.get("string_null_tokens"), str):
        lst = _to_list(merged.get("string_null_tokens"))
        if lst is not None:
            merged["string_null_tokens"] = lst

    return merged


def select_table_mapping(raw_cfg: Dict[str, Any], source_table_name: str | None) -> Dict[str, Any] | None:
    """
    从原始配置中选择映射规则（仅返回原始映射片段，合并 default 和表级）。
    若不存在 mappings 段，返回 None。
    合并规则：default 作为基，若表级匹配 source_table_name 则覆盖对应键。
    """
    mappings = raw_cfg.get("mappings") if isinstance(raw_cfg, dict) else None
    if not isinstance(mappings, dict):
        return None

    result: Dict[str, Any] = {}
    default_map = mappings.get("default") or {}
    if isinstance(default_map, dict):
        result.update(default_map)

    if source_table_name:
        tbl_list = mappings.get("tables") or []
        if isinstance(tbl_list, list):
            for item in tbl_list:
                if not isinstance(item, dict):
                    continue
                if str(item.get("source_table", "")).strip().lower() == str(source_table_name).strip().lower():
                    result.update(item)
                    break

    # 规范化部分字段的类型（列表/布尔等）
    for list_key in ["include", "exclude", "order", "string_null_tokens"]:
        if list_key in result and isinstance(result[list_key], str):
            lst = _to_list(result[list_key])
            if lst is not None:
                result[list_key] = lst

    for bool_key in []:
        if bool_key in result and isinstance(result[bool_key], str):
            bv = _to_bool(result[bool_key])
            if bv is not None:
                result[bool_key] = bv

    return result or None


