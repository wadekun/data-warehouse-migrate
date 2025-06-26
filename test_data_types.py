#!/usr/bin/env python3
"""
测试数据类型转换的脚本
"""

import pandas as pd
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.migrator import DataMigrator


def test_dataframe_optimization():
    """测试DataFrame类型优化"""
    
    # 创建测试数据
    test_data = {
        'int_as_string': ['1', '2', '3', '4', '5'],
        'float_as_string': ['1.5', '2.7', '3.14', '4.0', '5.5'],
        'mixed_types': ['1', 2, '3.0', 4.5, 'text'],
        'boolean_as_string': ['true', 'false', '1', '0', 'yes'],
        'normal_string': ['hello', 'world', 'test', 'data', 'migration'],
        'with_nulls': ['1', None, '3', 'null', '5'],
    }
    
    df = pd.DataFrame(test_data)
    print("原始DataFrame:")
    print(df.dtypes)
    print(df.head())
    print()
    
    # 创建迁移器实例（不需要真实连接）
    migrator = DataMigrator(
        source_project_id="test",
        destination_project_id="test"
    )
    
    # 测试类型优化
    try:
        optimized_df = migrator._optimize_dataframe_types(df)
        print("优化后的DataFrame:")
        print(optimized_df.dtypes)
        print(optimized_df.head())
        print()
        
        print("类型转换成功！")
        return True
        
    except Exception as e:
        print(f"类型优化失败: {e}")
        return False


if __name__ == '__main__':
    success = test_dataframe_optimization()
    sys.exit(0 if success else 1)
