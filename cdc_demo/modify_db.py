#!/usr/bin/env python3
import time
import random
import pymysql

# MySQL Config
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'debezium',  # Using the user we created in init.sql
    'password': 'dbz',
    'database': 'inventory',
    'autocommit': True
}

def get_connection():
    return pymysql.connect(**DB_CONFIG)

def modify_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    actions = ['insert', 'update', 'delete']
    
    print("🚀 Starting Database Activity Simulator...")
    
    try:
        while True:
            action = random.choices(actions, weights=[0.6, 0.3, 0.1])[0]
            
            if action == 'insert':
                name = f"User_{random.randint(1000, 9999)}"
                email = f"{name}@example.com"
                role = random.choice(['admin', 'editor', 'user'])
                cursor.execute("INSERT INTO users (name, email, role) VALUES (%s, %s, %s)", (name, email, role))
                print(f"➕ Inserted: {name}")
                
            elif action == 'update':
                # Update a random user
                cursor.execute("SELECT id FROM users ORDER BY RAND() LIMIT 1")
                res = cursor.fetchone()
                if res:
                    uid = res[0]
                    new_role = random.choice(['admin', 'editor', 'user', 'banned'])
                    cursor.execute("UPDATE users SET role = %s WHERE id = %s", (new_role, uid))
                    print(f"🔄 Updated User ID {uid} -> Role: {new_role}")
            
            elif action == 'delete':
                # Delete a random user
                cursor.execute("SELECT id FROM users ORDER BY RAND() LIMIT 1")
                res = cursor.fetchone()
                if res:
                    uid = res[0]
                    cursor.execute("DELETE FROM users WHERE id = %s", (uid,))
                    print(f"❌ Deleted User ID {uid}")
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    modify_data()
