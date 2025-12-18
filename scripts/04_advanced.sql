-- DuckLake Advanced Features Demo
-- Demonstrates schema evolution, transactions, and more

-- ============================================
-- SCHEMA EVOLUTION
-- ============================================

-- Add a new column
ALTER TABLE products ADD COLUMN description VARCHAR;

-- View the updated schema
DESCRIBE products;

-- Add data with the new column
UPDATE products SET description = 'High-performance laptop computer' WHERE id = 1;
UPDATE products SET description = 'Mechanical keyboard with RGB' WHERE id = 2;

-- Verify the changes
SELECT id, name, description FROM products;

-- Rename a column
-- ALTER TABLE products RENAME COLUMN stock TO quantity;

-- ============================================
-- MULTI-TABLE TRANSACTIONS
-- ============================================

-- Create an orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER,
    order_date DATE DEFAULT CURRENT_DATE,
    status VARCHAR DEFAULT 'pending'
);

-- Create a transaction log table
CREATE TABLE IF NOT EXISTS transaction_log (
    log_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    action VARCHAR,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert related data (DuckLake supports ACID transactions)
INSERT INTO orders (order_id, product_id, quantity, status) VALUES (1, 1, 2, 'completed');
INSERT INTO transaction_log (log_id, order_id, action) VALUES (1, 1, 'order_created');

-- Verify both tables were updated atomically
SELECT * FROM orders;
SELECT * FROM transaction_log;

-- ============================================
-- METADATA INSPECTION
-- ============================================

-- View all tables in the DuckLake catalog
SELECT * FROM ducklake_tables('my_lake');

-- View table metadata
SELECT * FROM ducklake_table_info('my_lake', 'products');

-- View data files for a table
SELECT * FROM ducklake_table_files('my_lake', 'products');

-- ============================================
-- COMPACTION (if supported)
-- ============================================

-- Compact small files into larger ones for better query performance
-- CALL ducklake_compact('my_lake', 'products');

-- ============================================
-- CLEANUP: Remove old snapshots (use with caution!)
-- ============================================

-- To reclaim storage, you can expire old snapshots
-- CALL ducklake_expire_snapshots('my_lake', 'products', OLDER_THAN => '7 days');
