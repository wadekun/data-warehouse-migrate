"""
测试数据类型映射模块
"""

import pytest
from google.cloud import bigquery

from data_warehouse_migrate.schema_mapper import SchemaMapper
from data_warehouse_migrate.exceptions import SchemaConversionError


class TestSchemaMapper:
    """测试SchemaMapper类"""
    
    def test_basic_type_conversion(self):
        """测试基础数据类型转换"""
        maxcompute_columns = [
            {'name': 'id', 'type': 'bigint', 'comment': '主键'},
            {'name': 'name', 'type': 'string', 'comment': '姓名'},
            {'name': 'age', 'type': 'int', 'comment': '年龄'},
            {'name': 'score', 'type': 'double', 'comment': '分数'},
            {'name': 'is_active', 'type': 'boolean', 'comment': '是否激活'},
            {'name': 'created_at', 'type': 'datetime', 'comment': '创建时间'},
        ]
        
        bigquery_fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(maxcompute_columns)
        
        assert len(bigquery_fields) == 6
        
        # 检查字段名和类型
        field_dict = {field.name: field for field in bigquery_fields}
        
        assert field_dict['id'].field_type == bigquery.enums.SqlTypeNames.INT64
        assert field_dict['name'].field_type == bigquery.enums.SqlTypeNames.STRING
        assert field_dict['age'].field_type == bigquery.enums.SqlTypeNames.INT64
        assert field_dict['score'].field_type == bigquery.enums.SqlTypeNames.FLOAT64
        assert field_dict['is_active'].field_type == bigquery.enums.SqlTypeNames.BOOL
        assert field_dict['created_at'].field_type == bigquery.enums.SqlTypeNames.DATETIME
        
        # 检查描述
        assert field_dict['id'].description == '主键'
        assert field_dict['name'].description == '姓名'
    
    def test_array_type_conversion(self):
        """测试数组类型转换"""
        maxcompute_columns = [
            {'name': 'tags', 'type': 'array<string>', 'comment': '标签数组'},
            {'name': 'scores', 'type': 'array<double>', 'comment': '分数数组'},
        ]
        
        bigquery_fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(maxcompute_columns)
        
        assert len(bigquery_fields) == 2
        
        field_dict = {field.name: field for field in bigquery_fields}
        
        # 检查数组字段
        assert field_dict['tags'].field_type == bigquery.enums.SqlTypeNames.STRING
        assert field_dict['tags'].mode == 'REPEATED'
        
        assert field_dict['scores'].field_type == bigquery.enums.SqlTypeNames.FLOAT64
        assert field_dict['scores'].mode == 'REPEATED'
    
    def test_map_type_conversion(self):
        """测试Map类型转换"""
        maxcompute_columns = [
            {'name': 'metadata', 'type': 'map<string,string>', 'comment': '元数据'},
        ]
        
        bigquery_fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(maxcompute_columns)
        
        assert len(bigquery_fields) == 1
        
        field = bigquery_fields[0]
        assert field.name == 'metadata'
        assert field.field_type == bigquery.enums.SqlTypeNames.RECORD
        assert field.mode == 'NULLABLE'
        assert len(field.fields) == 2
        
        # 检查子字段
        subfield_dict = {f.name: f for f in field.fields}
        assert 'key' in subfield_dict
        assert 'value' in subfield_dict
        assert subfield_dict['key'].field_type == bigquery.enums.SqlTypeNames.STRING
        assert subfield_dict['value'].field_type == bigquery.enums.SqlTypeNames.STRING
    
    def test_unknown_type_conversion(self):
        """测试未知类型转换"""
        maxcompute_columns = [
            {'name': 'unknown_field', 'type': 'unknown_type', 'comment': '未知类型'},
        ]
        
        bigquery_fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(maxcompute_columns)
        
        assert len(bigquery_fields) == 1
        
        field = bigquery_fields[0]
        assert field.name == 'unknown_field'
        assert field.field_type == bigquery.enums.SqlTypeNames.STRING  # 默认转换为STRING
    
    def test_decimal_type_conversion(self):
        """测试decimal类型转换"""
        maxcompute_columns = [
            {'name': 'price', 'type': 'decimal(10,2)', 'comment': '价格'},
        ]
        
        bigquery_fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(maxcompute_columns)
        
        assert len(bigquery_fields) == 1
        
        field = bigquery_fields[0]
        assert field.name == 'price'
        assert field.field_type == bigquery.enums.SqlTypeNames.NUMERIC

    def test_decimal_within_numeric_bounds(self):
        """decimal(p<=38, s<=9) 应映射为 NUMERIC"""
        columns = [
            {'name': 'a', 'type': 'decimal(10,2)', 'comment': ''},
            {'name': 'b', 'type': 'decimal(38,9)', 'comment': ''},
            {'name': 'c', 'type': 'decimal(5,0)', 'comment': ''},
        ]
        fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(columns)
        for f in fields:
            assert f.field_type == bigquery.enums.SqlTypeNames.NUMERIC, f.name

    def test_decimal_requires_bignumeric_on_scale_overflow(self):
        """decimal scale>9 必须升级为 BIGNUMERIC（这是原 bug 场景）"""
        columns = [
            {'name': 'gross_revenue', 'type': 'decimal(38,18)', 'comment': ''},
            {'name': 'big', 'type': 'decimal(76,38)', 'comment': ''},
        ]
        fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(columns)
        for f in fields:
            assert f.field_type == bigquery.enums.SqlTypeNames.BIGNUMERIC, f.name

    def test_decimal_requires_bignumeric_on_precision_overflow(self):
        """decimal precision>38 必须升级为 BIGNUMERIC"""
        columns = [
            {'name': 'x', 'type': 'decimal(50,4)', 'comment': ''},
        ]
        fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(columns)
        assert fields[0].field_type == bigquery.enums.SqlTypeNames.BIGNUMERIC

    def test_decimal_without_precision_uses_bignumeric(self):
        """未显式指定精度的 decimal，按更宽容的 BIGNUMERIC 处理"""
        columns = [
            {'name': 'x', 'type': 'decimal', 'comment': ''},
        ]
        fields = SchemaMapper.convert_maxcompute_to_bigquery_schema(columns)
        assert fields[0].field_type == bigquery.enums.SqlTypeNames.BIGNUMERIC

    def test_parse_decimal_params(self):
        """内部 decimal 参数解析"""
        assert SchemaMapper._parse_decimal_params('decimal(38,18)') == (38, 18)
        assert SchemaMapper._parse_decimal_params('DECIMAL(10, 2)') == (10, 2)
        assert SchemaMapper._parse_decimal_params('decimal') == (None, None)
        assert SchemaMapper._parse_decimal_params('bigint') == (None, None)
