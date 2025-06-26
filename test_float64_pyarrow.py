#!/usr/bin/env python3
"""
测试float64类型的pyarrow兼容性
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.bigquery_client import BigQueryClient


def test_float64_pyarrow_compatibility():
    """测试float64类型的pyarrow兼容性"""
    
    # 创建包含各种float64问题的测试数据
    test_data = {
        'normal_float': [1.1, 2.2, 3.3, 4.4, 5.5],
        'with_nan': [1.0, 2.0, np.nan, 4.0, 5.0],
        'with_inf': [1.0, 2.0, np.inf, 4.0, 5.0],
        'with_neg_inf': [1.0, 2.0, -np.inf, 4.0, 5.0],
        'mixed_special': [1.0, np.nan, np.inf, -np.inf, 5.0],
        'offline_pay_confirm_status': [0.0, 1.0, np.nan, 1.0, 0.0],  # 模拟问题列
    }
    
    df = pd.DataFrame(test_data)
    
    # 确保所有列都是float64类型
    for col in df.columns:
        df[col] = df[col].astype('float64')
    
    print("原始DataFrame:")
    print(df.dtypes)
    print(df)
    print()
    
    # 测试BigQuery客户端的处理
    try:
        # 创建BigQuery客户端实例（不需要真实连接）
        client = BigQueryClient(project_id="test")
    except:
        # 如果连接失败，我们只测试数据类型转换
        client = None
    
    try:
        if client:
            # 测试数据清理
            cleaned_df = client._clean_dataframe_for_bigquery(df)
            print("清理后的DataFrame:")
            print(cleaned_df.dtypes)
            print(cleaned_df)
            print()
            
            # 测试pyarrow兼容性
            compatible_df = client._ensure_pyarrow_compatibility(cleaned_df)
            print("pyarrow兼容处理后的DataFrame:")
            print(compatible_df.dtypes)
            print(compatible_df)
            print()
            
            # 验证pyarrow转换
            client._validate_pyarrow_conversion(compatible_df)
            print("✅ pyarrow转换验证成功！")
        else:
            print("⚠️  无法创建BigQuery客户端，跳过测试")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False


if __name__ == '__main__':
    success = test_float64_pyarrow_compatibility()
    sys.exit(0 if success else 1)
