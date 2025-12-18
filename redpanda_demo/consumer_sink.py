#!/usr/bin/env python3
import time
import json
import pandas as pd
import duckdb
from kafka import KafkaConsumer
from pathlib import Path

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'orders'
BATCH_SIZE = 100       # Flush every 100 messages
BATCH_TIMEOUT = 10.0   # Or flush every 10 seconds

# DuckLake Paths
PROJECT_DIR = Path(__file__).parent.parent
METADATA_PATH = PROJECT_DIR / "metadata" / "sales_by_date.ducklake"
DATA_PATH = PROJECT_DIR / "data_by_date"

def setup_duckdb():
    """Connect to DuckDB/DuckLake"""
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    
    # Attach DuckLake
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' AS sales_lake (DATA_PATH '{DATA_PATH}/', OVERRIDE_DATA_PATH true)
    """)
    
    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_lake.stream_orders (
            order_id VARCHAR,
            order_time TIMESTAMP,
            customer_id VARCHAR,
            product VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            region VARCHAR,
            ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    return conn

def flush_batch(conn, batch_data):
    """Write batch to DuckLake"""
    if not batch_data:
        return

    df = pd.DataFrame(batch_data)
    # Convert epoch to datetime
    df['order_time'] = pd.to_datetime(df['order_time'], unit='s')
    
    print(f"💾 Flushing {len(df)} records to DuckLake...", end='', flush=True)
    
    try:
        # Use DuckDB to insert DataFrame directly
        conn.execute("INSERT INTO sales_lake.stream_orders SELECT *, CURRENT_TIMESTAMP FROM df")
        print(" ✅ Done")
    except Exception as e:
        print(f" ❌ Error: {e}")

def main():
    print(f"🚀 Starting Consumer Sink for '{TOPIC}'...")
    
    # Connect DuckDB
    try:
        duck_conn = setup_duckdb()
    except Exception as e:
        print(f"❌ Failed to connect to DuckDB: {e}")
        return

    # Connect Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"❌ Failed to connect to Redpanda: {e}")
        return

    print("📥 Waiting for messages...")
    
    batch_buffer = []
    last_flush_time = time.time()
    
    try:
        for message in consumer:
            batch_buffer.append(message.value)
            
            # Check flush conditions
            current_time = time.time()
            time_since_flush = current_time - last_flush_time
            
            if len(batch_buffer) >= BATCH_SIZE or time_since_flush >= BATCH_TIMEOUT:
                flush_batch(duck_conn, batch_buffer)
                batch_buffer = []  # Clear buffer
                last_flush_time = current_time
                
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped.")
    finally:
        # Flush remaining
        if batch_buffer:
            print("Cleaning up remaining messages...")
            flush_batch(duck_conn, batch_buffer)
        
        duck_conn.close()
        consumer.close()

if __name__ == '__main__':
    main()
