-- DuckLake Time Travel Demo
-- Demonstrates time travel capabilities

-- ============================================
-- View Snapshot History
-- ============================================

-- List all snapshots (versions) of the database
SELECT * FROM ducklake_snapshots('my_lake');

-- List snapshots for a specific table
SELECT * FROM ducklake_table_snapshots('my_lake', 'products');

-- ============================================
-- Query Historical Data
-- ============================================

-- Query data at a specific version
-- Replace VERSION_NUMBER with an actual snapshot ID from above
-- SELECT * FROM products AT (VERSION => 1);

-- Query data at a specific timestamp
-- SELECT * FROM products AT (TIMESTAMP => '2024-01-01 12:00:00');

-- ============================================
-- Compare Versions
-- ============================================

-- Get current data
SELECT 'Current' as version, * FROM products;

-- Get data at version 1 (initial insert)
-- SELECT 'Version 1' as version, * FROM products AT (VERSION => 1);

-- ============================================
-- Change Data Feed
-- ============================================

-- View changes between snapshots
-- Shows inserts, updates, and deletes between versions
-- SELECT * FROM table_changes('products', 1, 3);

-- Example with specific change types
-- SELECT * FROM table_changes('products', 1, 3) 
-- WHERE change_type = 'insert';
