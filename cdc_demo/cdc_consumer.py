#!/usr/bin/env python3
import json
import duckdb
from kafka import KafkaConsumer
from pathlib import Path

# Paths
PROJECT_DIR = Path(__file__).parent.parent
METADATA_PATH = PROJECT_DIR / "metadata" / "sales_by_date.ducklake"
DATA_PATH = PROJECT_DIR / "data_by_date"

# Config
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'dbserver1.inventory.users'  # Debezium default topic naming: prefix.db.table

def setup_ducklake():
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    
    # Attach DuckLake
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' AS my_lake (DATA_PATH '{DATA_PATH}/', OVERRIDE_DATA_PATH true)
    """)
    
    # Create target table (matching MySQL schema)
    # Note: Simplified schema for demo
    conn.execute("""
        CREATE TABLE IF NOT EXISTS my_lake.users (
            id INT,
            name VARCHAR,
            email VARCHAR,
            role VARCHAR,
            op VARCHAR,  -- Operation type: c=create, u=update, d=delete
            ts_ms BIGINT
        )
    """)
    return conn

def process_event(conn, event):
    if event is None or event['payload'] is None:
        return

    payload = event['payload']
    op = payload['op']
    
    # Extract data based on operation
    if op in ('c', 'r'):  # Create or Read (Snapshot)
        data = payload['after']
        conn.execute("INSERT INTO my_lake.users VALUES (?, ?, ?, ?, ?, ?)", 
                     [data['id'], data['name'], data['email'], data['role'], op, payload['ts_ms']])
        print(f"➕ Inserted: {data['name']}")
        
    elif op == 'u':  # Update
        # In a real Lakehouse, handle Merge/Upsert. 
        # Here we just append the new version (log-based).
        data = payload['after']
        conn.execute("INSERT INTO my_lake.users VALUES (?, ?, ?, ?, ?, ?)", 
                     [data['id'], data['name'], data['email'], data['role'], op, payload['ts_ms']])
        print(f"🔄 Updated: {data['name']}")
        
    elif op == 'd':  # Delete
        data = payload['before']
        # Soft delete marker
        conn.execute("INSERT INTO my_lake.users VALUES (?, ?, ?, ?, ?, ?)", 
                     [data['id'], data['name'], data['email'], data['role'], op, payload['ts_ms']])
        print(f"❌ Deleted ID: {data['id']}")

def main():
    print(f"🚀 Starting CDC Consumer for topic '{TOPIC}'...")
    
    conn = setup_ducklake()
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )

    print("📥 Waiting for change events...")
    
    try:
        for message in consumer:
            if message.value:
                process_event(conn, message.value)
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
    finally:
        conn.close()
        consumer.close()

if __name__ == '__main__':
    main()
