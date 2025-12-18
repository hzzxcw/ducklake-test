#!/usr/bin/env python3
import time
import json
import random
import uuid
from kafka import KafkaProducer

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'orders'

def generate_order():
    """Generate a random sales order"""
    products = ['iPhone 15', 'MacBook Pro', 'iPad Air', 'AirPods Pro', 'Apple Watch', 
                'Samsung TV', 'Sony Camera', 'Nintendo Switch', 'PS5', 'Xbox']
    regions = ['华东', '华南', '华北', '华中', '西南', '西北']
    
    return {
        'order_id': str(uuid.uuid4()),
        'order_time': int(time.time()),
        'customer_id': f"CUST-{random.randint(1, 500):04d}",
        'product': random.choice(products),
        'quantity': random.randint(1, 5),
        'unit_price': float(random.randint(100, 9999)),
        'region': random.choice(regions)
    }

def main():
    print(f"🚀 Starting Producer to {BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Failed to connect to Redpanda: {e}")
        return

    print(f"📦 Sending messages to topic '{TOPIC}'...")
    count = 0
    try:
        while True:
            order = generate_order()
            producer.send(TOPIC, order)
            
            count += 1
            if count % 10 == 0:
                print(f"Sent {count} orders...", end='\r', flush=True)
                
            time.sleep(0.1)  # Simulate 10 orders/sec
            
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped.")
    finally:
        producer.close()

if __name__ == '__main__':
    main()
