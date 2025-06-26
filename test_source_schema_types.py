#!/usr/bin/env python3
"""
测试基于源表结构的数据类型处理
"""

import pandas as pd
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def test_source_schema_type_conversion():
    """测试基于源表结构的类型转换"""
    
    # 模拟源表结构
    mock_source_columns = [
        {'name': 'id', 'type': 'bigint', 'comment': '主键', 'is_partition': False},
        {'name': 'offline_pay_confirm_status', 'type': 'string', 'comment': '离线支付确认状态', 'is_partition': False},
        {'name': 'amount', 'type': 'double', 'comment': '金额', 'is_partition': False},
        {'name': 'is_active', 'type': 'boolean', 'comment': '是否激活', 'is_partition': False},
        {'name': 'pt', 'type': 'string', 'comment': '分区字段', 'is_partition': True},
    ]
    
    # 模拟从MaxCompute读取的数据（都是object类型）
    test_data = {
        'id': ['1', '2', '3', '4', '5'],
        'offline_pay_confirm_status': ['0', '1', None, '0', '1'],  # 字符串类型，包含null
        'amount': ['10.5', '20.0', '30.5', None, '50.0'],
        'is_active': ['true', 'false', 'true', None, 'false'],
        'pt': ['20241226', '20241226', '20241226', '20241226', '20241226'],
    }
    
    df = pd.DataFrame(test_data)
    
    print("原始DataFrame（模拟从MaxCompute读取的数据）:")
    print("数据类型:", df.dtypes.to_dict())
    print("数据内容:")
    print(df)
    print()
    
    # 模拟DataMigrator的类型转换逻辑
    try:
        from data_warehouse_migrate.migrator import DataMigrator
        
        # 创建一个模拟的migrator（不需要真实连接）
        class MockMaxComputeClient:
            def get_table_schema(self, table_name):
                return mock_source_columns
        
        migrator = DataMigrator("test", "test")
        migrator.maxcompute_client = MockMaxComputeClient()
        
        # 测试类型转换
        typed_df = migrator._apply_source_schema_types(df, "test_table")
        
        print("转换后的DataFrame（基于源表结构）:")
        print("数据类型:", typed_df.dtypes.to_dict())
        print("数据内容:")
        print(typed_df)
        print()
        
        # 验证转换结果
        print("验证结果:")
        print(f"✓ id列类型: {typed_df['id'].dtype} (期望: int64 或 float64)")
        print(f"✓ offline_pay_confirm_status列类型: {typed_df['offline_pay_confirm_status'].dtype} (期望: object/string)")
        print(f"✓ amount列类型: {typed_df['amount'].dtype} (期望: float64)")
        print(f"✓ is_active列类型: {typed_df['is_active'].dtype} (期望: object)")
        
        # 检查offline_pay_confirm_status是否保持为字符串类型
        if typed_df['offline_pay_confirm_status'].dtype == 'object':
            print("🎉 成功！offline_pay_confirm_status保持为字符串类型")
            return True
        else:
            print(f"❌ 失败！offline_pay_confirm_status被错误转换为: {typed_df['offline_pay_confirm_status'].dtype}")
            return False
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_source_schema_type_conversion()
    sys.exit(0 if success else 1)
