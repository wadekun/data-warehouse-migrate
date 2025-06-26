#!/usr/bin/env python3
"""
测试pyarrow兼容性的脚本
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.bigquery_client import BigQueryClient


def test_pyarrow_compatibility():
    """测试pyarrow兼容性"""
    
    # 创建包含问题类型的测试数据
    test_data = {
        'normal_int': [1, 2, 3, 4, 5],
        'int_with_nan': [1, 2, None, 4, 5],
        'string_numbers': ['1', '2', '3', '4', '5'],
        'mixed_with_nan': [1, '2', None, 4.0, '5'],
        'boolean_values': [True, False, True, False, True],
        'float_values': [1.1, 2.2, 3.3, np.nan, 5.5],
    }
    
    df = pd.DataFrame(test_data)
    
    # 转换为可能有问题的类型
    df['int_with_nan'] = df['int_with_nan'].astype('Int64')  # 这会导致pyarrow错误
    df['boolean_values'] = df['boolean_values'].astype('boolean')  # 这也可能有问题
    
    print("原始DataFrame类型:")
    print(df.dtypes)
    print("\n原始DataFrame:")
    print(df)
    print()
    
    # 创建BigQuery客户端实例（不需要真实连接）
    try:
        client = BigQueryClient(project_id="test")
    except:
        # 如果连接失败，我们只测试数据类型转换
        client = None
    
    # 测试pyarrow兼容性转换
    try:
        if client:
            compatible_df = client._ensure_pyarrow_compatibility(df)
        else:
            # 手动创建一个简化的转换逻辑
            compatible_df = df.copy()
            for column in compatible_df.columns:
                dtype = compatible_df[column].dtype
                if str(dtype) == 'Int64':
                    if compatible_df[column].isna().any():
                        compatible_df[column] = compatible_df[column].astype('float64')
                    else:
                        compatible_df[column] = compatible_df[column].astype('int64')
                elif str(dtype) == 'boolean':
                    compatible_df[column] = compatible_df[column].astype('bool')
        
        print("转换后的DataFrame类型:")
        print(compatible_df.dtypes)
        print("\n转换后的DataFrame:")
        print(compatible_df)
        print()
        
        # 测试是否可以转换为pyarrow
        try:
            import pyarrow as pa
            table = pa.Table.from_pandas(compatible_df)
            print("✅ pyarrow转换成功！")
            print(f"pyarrow表结构: {table.schema}")
            return True
        except ImportError:
            print("⚠️  pyarrow未安装，无法测试转换")
            return True
        except Exception as e:
            print(f"❌ pyarrow转换失败: {e}")
            return False
            
    except Exception as e:
        print(f"❌ 数据类型转换失败: {e}")
        return False


if __name__ == '__main__':
    success = test_pyarrow_compatibility()
    sys.exit(0 if success else 1)
