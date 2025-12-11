# 类型映射注意事项

本文档说明数据迁移过程中的类型映射特殊情况和处理方法。

## MySQL到MaxCompute的特殊类型处理

### 1. TINYINT(1) 自动转换为BOOLEAN

**自动行为**：
- 非空的 `tinyint(1)` 会自动转换为 MaxCompute 的 `boolean` 类型
- 可空的 `tinyint(1)` 会保持为 `tinyint` 类型

**原因**：
- 在MySQL中，`tinyint(1)` 常被用作布尔值（0/1）
- 这是约定俗成的用法，但并非强制要求

**注意事项**：
- 如果你的 `tinyint(1)` 列实际存储的是数值（如-128到127），自动转换可能会丢失信息
- 建议检查这些列的实际用途

**日志输出示例**：
```
WARNING: 列 is_active: MySQL tinyint(1) 被推断为布尔类型，转换为MaxCompute boolean。如果该列用于存储数值，请手动覆盖类型映射
```

### 2. ENUM 和 SET 类型

- 自动转换为 `string` 类型
- 枚举值会作为字符串存储

### 3. 自增主键

- MaxCompute 不支持自增属性
- 自增列会作为普通整数列迁移
- 需要在应用层面处理主键生成

### 4. 默认值

- MaxCompute 不支持列默认值（除了NULL）
- MySQL的默认值会被忽略

## 手动类型覆盖

如果自动转换不符合预期，可以通过配置文件手动覆盖类型：

### 配置文件示例

```json
{
  "source": {
    "type": "mysql",
    "mysql": {
      "host": "localhost",
      "user": "root",
      "password": "password",
      "database": "mydb"
    },
    "table_name": "users"
  },
  "destination": {
    "type": "maxcompute",
    "table_name": "ods_users"
  },
  "mappings": {
    "users": {
      "type_overrides": {
        "is_active": "tinyint",
        "status_flag": "tinyint"
      }
    }
  }
}
```

### 常见类型覆盖场景

1. **保持 TINYINT 为整数**：
   ```json
   "type_overrides": {
     "is_active": "tinyint"  // 不转换为boolean
   }
   ```

2. **强制转换类型**：
   ```json
   "type_overrides": {
     "price": "decimal(18,4)",
     "created_date": "date"
   }
   ```

3. **VARCHAR 长度调整**：
   ```json
   "type_overrides": {
     "description": "string",
     "short_code": "string"
   }
   ```

## 类型转换参考表

| MySQL 类型 | MaxCompute 类型 | 说明 |
|-----------|----------------|------|
| TINYINT | tinyint | 保持原样 |
| TINYINT(1) | boolean | 非空时自动转换 |
| SMALLINT | smallint | 直接转换 |
| INT/MEDIUMINT | int | 直接转换 |
| BIGINT | bigint | 直接转换 |
| FLOAT | float | 直接转换 |
| DOUBLE | double | 直接转换 |
| DECIMAL(p,s) | decimal(p,s) | 保留精度 |
| VARCHAR(n) | string | 忽略长度限制 |
| CHAR(n) | char(n) | 保留长度 |
| TEXT/LONGTEXT | string | 转换 |
| DATE | date | 直接转换 |
| DATETIME | datetime | 直接转换 |
| TIMESTAMP | timestamp | 直接转换 |
| ENUM | string | 枚举值作为字符串 |
| SET | string | 集合值作为字符串 |
| BLOB/BINARY | binary | 二进制数据 |
| JSON | string | JSON作为字符串存储 |

## 最佳实践

1. **迁移前验证**
   - 使用 `--dry-run` 参数查看类型转换结果
   - 检查日志中的 WARNING 信息

2. **测试数据**
   - 先迁移少量数据进行验证
   - 特别注意 `tinyint(1)` 列的值范围

3. **性能考虑**
   - 避免使用过长的VARCHAR
   - 合理使用分区（如果需要）

4. **数据一致性**
   - 迁移后进行数据校验
   - 特别关注数值类型的精度保持

## 常见问题

### Q: tinyint(1) 存储了 0-255 的值，如何避免转换为boolean？
A: 在配置文件中使用 type_overrides 明确指定为 tinyint。

### Q: MySQL的JSON类型如何处理？
A: 转换为string类型，在MaxCompute中存储为文本。

### Q: 如何处理自增主键？
A: 自增属性会被移除，如需保持唯一性，建议在MaxCompute中使用UUID或其他算法生成。

### Q: 默认值会迁移吗？
A: 不会，MaxCompute不支持列默认值。需要在应用层面处理。