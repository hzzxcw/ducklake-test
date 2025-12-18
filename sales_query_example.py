#!/usr/bin/env python3
"""
DuckLake 按日期分区数据查询示例
生成模拟数据并查询过去一个月的销售统计
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_DIR = Path(__file__).parent
METADATA_PATH = PROJECT_DIR / "metadata" / "sales_by_date.ducklake"
DATA_PATH = PROJECT_DIR / "data_by_date"


def setup_ducklake(conn):
    """设置 DuckLake 连接"""
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    
    # 确保数据目录存在
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    
    # 附加 DuckLake
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' AS sales_lake (DATA_PATH '{DATA_PATH}/')
    """)


def generate_sample_data(conn, num_days=90, orders_per_day=100):
    """
    生成模拟销售数据 (过去 N 天)
    每天的数据会自动分到不同的 Parquet 文件
    """
    print(f"📦 生成模拟数据: 过去 {num_days} 天, 每天约 {orders_per_day} 条订单...")
    
    # 删除旧表重建
    conn.execute("DROP TABLE IF EXISTS sales_lake.daily_sales")
    
    # 创建表
    conn.execute("""
        CREATE TABLE sales_lake.daily_sales (
            order_id VARCHAR,
            order_date DATE,
            customer_id VARCHAR,
            product VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            amount DECIMAL(10,2),
            region VARCHAR
        )
    """)
    
    # 生成数据
    today = datetime.now().date()
    start_date = today - timedelta(days=num_days)
    
    conn.execute(f"""
        INSERT INTO sales_lake.daily_sales
        SELECT 
            'ORD-' || printf('%08d', row_number() OVER ()) as order_id,
            (DATE '{start_date}' + (random() * {num_days})::int)::DATE as order_date,
            'CUST-' || printf('%04d', (random() * 500)::int + 1) as customer_id,
            ['iPhone 15', 'MacBook Pro', 'iPad Air', 'AirPods Pro', 'Apple Watch', 
             'Samsung TV', 'Sony Camera', 'Nintendo Switch', 'PS5', 'Xbox'][1 + (random() * 10)::int % 10] as product,
            (random() * 5 + 1)::int as quantity,
            (random() * 900 + 100)::decimal(10,2) as unit_price,
            0.0 as amount,  -- 临时值
            ['华东', '华南', '华北', '华中', '西南', '西北'][1 + (random() * 6)::int % 6] as region
        FROM range({num_days * orders_per_day})
    """)
    
    # 更新 amount = quantity * unit_price
    conn.execute("""
        UPDATE sales_lake.daily_sales 
        SET amount = quantity * unit_price
    """)
    
    # 统计
    result = conn.execute("SELECT COUNT(*) FROM sales_lake.daily_sales").fetchone()
    print(f"✅ 生成 {result[0]:,} 条销售记录")
    
    # 显示日期范围
    date_range = conn.execute("""
        SELECT MIN(order_date) as min_date, MAX(order_date) as max_date 
        FROM sales_lake.daily_sales
    """).fetchone()
    print(f"📅 日期范围: {date_range[0]} 至 {date_range[1]}")


def query_last_month_sales(conn):
    """查询过去一个月的销售统计"""
    print("\n" + "=" * 60)
    print("📊 过去 30 天销售统计")
    print("=" * 60)
    
    # 基础统计
    print("\n【总体统计】")
    summary = conn.execute("""
        SELECT 
            COUNT(*) as 订单数,
            SUM(amount) as 总销售额,
            ROUND(AVG(amount), 2) as 平均订单金额,
            COUNT(DISTINCT customer_id) as 活跃客户数
        FROM sales_lake.daily_sales
        WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY
    """).fetchdf()
    print(summary.to_string(index=False))
    
    # 按日统计
    print("\n【每日销售趋势】(最近 10 天)")
    daily = conn.execute("""
        SELECT 
            order_date as 日期,
            COUNT(*) as 订单数,
            ROUND(SUM(amount), 2) as 日销售额,
            ROUND(AVG(amount), 2) as 平均订单额
        FROM sales_lake.daily_sales
        WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY order_date
        ORDER BY order_date DESC
        LIMIT 10
    """).fetchdf()
    print(daily.to_string(index=False))
    
    # 按产品统计
    print("\n【热销产品 Top 5】")
    products = conn.execute("""
        SELECT 
            product as 产品,
            SUM(quantity) as 销量,
            ROUND(SUM(amount), 2) as 销售额,
            COUNT(*) as 订单数
        FROM sales_lake.daily_sales
        WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY product
        ORDER BY 销售额 DESC
        LIMIT 5
    """).fetchdf()
    print(products.to_string(index=False))
    
    # 按区域统计
    print("\n【区域销售分布】")
    regions = conn.execute("""
        SELECT 
            region as 区域,
            COUNT(*) as 订单数,
            ROUND(SUM(amount), 2) as 销售额,
            ROUND(100.0 * SUM(amount) / (SELECT SUM(amount) FROM sales_lake.daily_sales 
                WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY), 1) as 占比
        FROM sales_lake.daily_sales
        WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY region
        ORDER BY 销售额 DESC
    """).fetchdf()
    print(regions.to_string(index=False))


def query_custom_date_range(conn, start_date: str, end_date: str):
    """查询自定义日期范围的销售数据"""
    print(f"\n📅 自定义查询: {start_date} 至 {end_date}")
    
    result = conn.execute(f"""
        SELECT 
            COUNT(*) as 订单数,
            ROUND(SUM(amount), 2) as 总销售额,
            ROUND(AVG(amount), 2) as 平均订单金额
        FROM sales_lake.daily_sales
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """).fetchdf()
    print(result.to_string(index=False))


def main():
    print("=" * 60)
    print("DuckLake 按日期查询销售数据示例")
    print("=" * 60)
    
    conn = duckdb.connect()
    setup_ducklake(conn)
    
    # 生成模拟数据 (90天, 每天100条)
    generate_sample_data(conn, num_days=90, orders_per_day=100)
    
    # 查询过去一个月
    query_last_month_sales(conn)
    
    # 自定义日期范围查询示例
    from datetime import date
    today = date.today()
    week_ago = today - timedelta(days=7)
    query_custom_date_range(conn, str(week_ago), str(today))
    
    print("\n" + "=" * 60)
    print("✅ 完成!")
    print("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
