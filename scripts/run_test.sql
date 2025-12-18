-- DuckLake Complete Test Script
-- Run with: duckdb < run_test.sql

INSTALL ducklake;
LOAD ducklake;

-- ========================================
-- SETUP: Attach DuckLake
-- ========================================
.print '=== Setting up DuckLake ==='
ATTACH 'ducklake:../metadata/my_lake.ducklake' AS my_lake (DATA_PATH '../data/');
USE my_lake;

-- ========================================
-- CREATE TABLE & INSERT DATA
-- ========================================
.print ''
.print '=== Creating Products Table ==='

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id INTEGER,
    name VARCHAR NOT NULL,
    category VARCHAR,
    price DECIMAL(10,2),
    stock INTEGER DEFAULT 0
);

INSERT INTO products VALUES
    (1, 'Laptop', 'Electronics', 999.99, 50),
    (2, 'Keyboard', 'Electronics', 79.99, 200),
    (3, 'Mouse', 'Electronics', 29.99, 500);

.print '=== Initial Data ==='
SELECT * FROM products;

-- ========================================
-- UPDATE DATA
-- ========================================
.print ''
.print '=== Updating Laptop Price to 899.99 ==='
UPDATE products SET price = 899.99 WHERE id = 1;
SELECT * FROM products;

-- ========================================
-- TIME TRAVEL
-- ========================================
.print ''
.print '=== Time Travel: Querying Previous Version ==='
SELECT * FROM products AT (VERSION => 1);

-- ========================================
-- CHANGE DATA FEED
-- ========================================
.print ''
.print '=== Change Data Feed ==='
SELECT * FROM my_lake.table_changes('products', 1, 2);

-- ========================================
-- METADATA INSPECTION
-- ========================================
.print ''
.print '=== DuckLake Tables ==='
SELECT * FROM ducklake_tables('my_lake');

.print ''
.print '=== DuckLake Snapshots ==='
SELECT * FROM ducklake_snapshots('my_lake');

.print ''
.print '=== Test Complete! ==='
