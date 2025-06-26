#!/usr/bin/env python3
"""
测试字符串类型保持的脚本
"""

import pandas as pd
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def test_string_type_preservation():
    """测试字符串类型保持"""
    
    # 模拟源表结构 - 包含容易被误判为数值的字符串字段
    mock_source_columns = [
        {'name': 'offline_pay_confirm_status', 'type': 'string', 'comment': '离线支付确认状态', 'is_partition': False},
        {'name': 'checkout_order_unique_code', 'type': 'string', 'comment': '订单唯一码', 'is_partition': False},
        {'name': 'order_id', 'type': 'bigint', 'comment': '订单ID', 'is_partition': False},
        {'name': 'amount', 'type': 'double', 'comment': '金额', 'is_partition': False},
    ]
    
    # 模拟从MaxCompute读取的数据（包含容易被误判的字符串值）
    test_data = {
        'offline_pay_confirm_status': ['0', '1', None, '0', '1', '2'],  # 数字字符串
        'checkout_order_unique_code': ['123456', '789012', '345678', None, '901234', '567890'],  # 纯数字字符串
        'order_id': ['1001', '1002', '1003', '1004', '1005', '1006'],  # 应该转换为数值
        'amount': ['10.5', '20.0', '30.5', None, '50.0', '60.5'],  # 应该转换为浮点数
    }
    
    df = pd.DataFrame(test_data)
    
    print("原始DataFrame（模拟从MaxCompute读取）:")
    print("数据类型:", df.dtypes.to_dict())
    print("数据内容:")
    print(df)
    print()
    
    # 测试类型转换
    try:
        from data_warehouse_migrate.migrator import DataMigrator
        
        # 创建模拟的MaxCompute客户端
        class MockMaxComputeClient:
            def get_table_schema(self, table_name):
                return mock_source_columns
        
        migrator = DataMigrator("test", "test")
        migrator.maxcompute_client = MockMaxComputeClient()
        
        # 应用源表结构类型
        typed_df = migrator._apply_source_schema_types(df, "test_table")
        
        print("转换后的DataFrame:")
        print("数据类型:", typed_df.dtypes.to_dict())
        print("数据内容:")
        print(typed_df)
        print()
        
        # 验证结果
        print("验证结果:")
        success = True
        
        # 检查字符串字段是否保持为字符串
        if typed_df['offline_pay_confirm_status'].dtype != 'object':
            print(f"❌ offline_pay_confirm_status 应该是字符串，实际是: {typed_df['offline_pay_confirm_status'].dtype}")
            success = False
        else:
            print(f"✓ offline_pay_confirm_status 正确保持为字符串类型")
        
        if typed_df['checkout_order_unique_code'].dtype != 'object':
            print(f"❌ checkout_order_unique_code 应该是字符串，实际是: {typed_df['checkout_order_unique_code'].dtype}")
            success = False
        else:
            print(f"✓ checkout_order_unique_code 正确保持为字符串类型")
        
        # 检查数值字段是否正确转换
        if typed_df['order_id'].dtype not in ['int64', 'float64']:
            print(f"❌ order_id 应该是数值类型，实际是: {typed_df['order_id'].dtype}")
            success = False
        else:
            print(f"✓ order_id 正确转换为数值类型: {typed_df['order_id'].dtype}")
        
        if typed_df['amount'].dtype != 'float64':
            print(f"❌ amount 应该是float64，实际是: {typed_df['amount'].dtype}")
            success = False
        else:
            print(f"✓ amount 正确转换为float64类型")
        
        if success:
            print("\n🎉 所有类型转换都正确！")
        else:
            print("\n❌ 存在类型转换错误")
        
        return success
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_string_type_preservation()
    sys.exit(0 if success else 1)
