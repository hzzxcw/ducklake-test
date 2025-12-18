#!/usr/bin/env python3
import duckdb
from pathlib import Path

# Paths
PROJECT_DIR = Path(__file__).parent.parent
METADATA_PATH = PROJECT_DIR / "metadata" / "sales_by_date.ducklake"
DATA_PATH = PROJECT_DIR / "data_by_date"

def main():
    print("=" * 50)
    print("🔍 DuckLake Stream Verification")
    print("=" * 50)
    
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{METADATA_PATH}' AS sales_lake (DATA_PATH '{DATA_PATH}/', OVERRIDE_DATA_PATH true)
        """)
        
        # Check table
        try:
            count = conn.execute("SELECT COUNT(*) FROM sales_lake.stream_orders").fetchone()[0]
            print(f"\n✅ Total Streamed Orders: {count}")
            
            if count > 0:
                print("\n📄 Latest 5 Records:")
                df = conn.execute("""
                    SELECT order_id, product, quantity, unit_price, order_time, ingest_time 
                    FROM sales_lake.stream_orders 
                    ORDER BY ingest_time DESC 
                    LIMIT 5
                """).fetchdf()
                print(df.to_string(index=False))
                
                print("\n📊 Sales by Product:")
                stats = conn.execute("""
                    SELECT product, COUNT(*) as count, SUM(quantity*unit_price) as revenue
                    FROM sales_lake.stream_orders
                    GROUP BY product
                    ORDER BY revenue DESC
                """).fetchdf()
                print(stats.to_string(index=False))
                
        except duckdb.CatalogException:
            print("\n⚠️ Table 'stream_orders' does not exist yet.")
            print("   (Start the consumer to create it)")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
