#!/usr/bin/env python3
"""
DuckLake Python Example
读取 DuckLake 数据并打印到终端
"""

import duckdb
from pathlib import Path

# 项目路径
PROJECT_DIR = Path(__file__).parent
METADATA_PATH = PROJECT_DIR / "metadata" / "my_lake.ducklake"
DATA_PATH = PROJECT_DIR / "data"

def main():
    # 连接 DuckDB (内存模式)
    conn = duckdb.connect()
    
    # 加载 DuckLake 扩展
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    
    # 附加 DuckLake 数据库
    # 使用 OVERRIDE_DATA_PATH 因为原来是用相对路径创建的
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' AS my_lake (
            DATA_PATH '{DATA_PATH}/',
            OVERRIDE_DATA_PATH true
        )
    """)
    
    print("=" * 60)
    print("DuckLake Python Example")
    print("=" * 60)
    
    # 1. 查询所有表
    print("\n📋 DuckLake 中的表:")
    tables = conn.execute("SHOW TABLES FROM my_lake").fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    # 2. 查询 customers 表
    print("\n👤 Customers 表 (前 5 条):")
    customers = conn.execute("""
        SELECT customer_id, name, city, tier 
        FROM my_lake.customers 
        LIMIT 5
    """).fetchdf()
    print(customers.to_string(index=False))
    
    # 3. 查询 sales 表
    print("\n💰 Sales 表 (前 5 条):")
    sales = conn.execute("""
        SELECT order_id, customer_id, product, amount, order_date::DATE as order_date
        FROM my_lake.sales 
        LIMIT 5
    """).fetchdf()
    print(sales.to_string(index=False))
    
    # 4. 聚合分析
    print("\n📊 销售统计 (按产品):")
    stats = conn.execute("""
        SELECT 
            product,
            COUNT(*) as order_count,
            ROUND(SUM(amount), 2) as total_amount,
            ROUND(AVG(amount), 2) as avg_amount
        FROM my_lake.sales
        GROUP BY product
        ORDER BY total_amount DESC
    """).fetchdf()
    print(stats.to_string(index=False))
    
    # 5. Time Travel 示例
    print("\n⏰ Time Travel - 查询历史版本:")
    snapshots = conn.execute("""
        SELECT snapshot_id, snapshot_time, changes
        FROM ducklake_snapshots('my_lake')
        ORDER BY snapshot_id DESC
        LIMIT 3
    """).fetchdf()
    print(snapshots.to_string(index=False))
    
    # 6. 对比当前和历史版本
    print("\n🔄 版本对比 - CUST-0001:")
    current = conn.execute("""
        SELECT 'Current' as version, customer_id, name, tier 
        FROM my_lake.customers 
        WHERE customer_id = 'CUST-0001'
    """).fetchdf()
    
    historical = conn.execute("""
        SELECT 'Version 5' as version, customer_id, name, tier 
        FROM my_lake.customers AT (VERSION => 5)
        WHERE customer_id = 'CUST-0001'
    """).fetchdf()
    
    import pandas as pd
    comparison = pd.concat([current, historical])
    print(comparison.to_string(index=False))
    
    print("\n" + "=" * 60)
    print("✅ 完成!")
    print("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
