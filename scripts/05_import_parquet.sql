-- Script to generate sample Parquet files and import into DuckLake
-- This simulates importing existing data into DuckLake for version management

-- ========================================
-- STEP 1: Generate sample Parquet files (simulating external data)
-- ========================================
.print '=== Step 1: Generating Sample Parquet Files ==='

-- Create a sample sales dataset
COPY (
    SELECT 
        row_number() OVER () as order_id,
        'CUST-' || printf('%04d', (random() * 1000)::int) as customer_id,
        ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse'][1 + (random() * 6)::int % 6] as product,
        (random() * 900 + 100)::decimal(10,2) as amount,
        DATE '2024-01-01' + INTERVAL ((random() * 365)::int) DAY as order_date
    FROM range(100)
) TO '../external_parquet/sales_2024.parquet' (FORMAT PARQUET);

-- Create a sample customers dataset
COPY (
    SELECT 
        'CUST-' || printf('%04d', i) as customer_id,
        ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry'][1 + i % 8] || ' ' || 
        ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'][1 + (i * 7) % 8] as name,
        ['Beijing', 'Shanghai', 'Shenzhen', 'Hangzhou', 'Guangzhou'][1 + i % 5] as city,
        ['VIP', 'Gold', 'Silver', 'Bronze'][1 + i % 4] as tier,
        DATE '2020-01-01' + INTERVAL (i * 3) DAY as join_date
    FROM range(1, 51) t(i)
) TO '../external_parquet/customers.parquet' (FORMAT PARQUET);

.print 'Generated: sales_2024.parquet (100 rows)'
.print 'Generated: customers.parquet (50 rows)'

-- Verify the generated files
.print ''
.print '=== Verifying Generated Parquet Files ==='
.print '--- Sales Data Preview ---'
SELECT * FROM read_parquet('../external_parquet/sales_2024.parquet') LIMIT 5;

.print '--- Customers Data Preview ---'
SELECT * FROM read_parquet('../external_parquet/customers.parquet') LIMIT 5;

-- ========================================
-- STEP 2: Import into DuckLake for version management
-- ========================================
.print ''
.print '=== Step 2: Importing into DuckLake ==='

INSTALL ducklake;
LOAD ducklake;

-- Attach DuckLake (reuse existing or create new)
ATTACH 'ducklake:../metadata/my_lake.ducklake' AS my_lake (DATA_PATH '../data/');

-- Import sales data
.print 'Importing sales data...'
DROP TABLE IF EXISTS my_lake.sales;
CREATE TABLE my_lake.sales AS
SELECT * FROM read_parquet('../external_parquet/sales_2024.parquet');

-- Import customers data
.print 'Importing customers data...'
DROP TABLE IF EXISTS my_lake.customers;
CREATE TABLE my_lake.customers AS
SELECT * FROM read_parquet('../external_parquet/customers.parquet');

.print 'Import complete!'

-- ========================================
-- STEP 3: Verify imported data
-- ========================================
.print ''
.print '=== Step 3: Verify Imported Data ==='

.print '--- Sales table info ---'
SELECT COUNT(*) as row_count FROM my_lake.sales;

.print '--- Customers table info ---'
SELECT COUNT(*) as row_count FROM my_lake.customers;

-- ========================================
-- STEP 4: Demonstrate version management
-- ========================================
.print ''
.print '=== Step 4: Demonstrating Version Management ==='

-- Make some updates
.print 'Making updates to test version management...'
UPDATE my_lake.customers SET tier = 'VIP' WHERE customer_id = 'CUST-0001';
UPDATE my_lake.sales SET amount = amount * 1.1 WHERE product = 'Laptop';

.print '--- Snapshots after import and updates ---'
SELECT snapshot_id, snapshot_time, changes 
FROM ducklake_snapshots('my_lake') 
ORDER BY snapshot_id DESC 
LIMIT 5;

-- Time travel example
.print ''
.print '--- Customer CUST-0001 current state ---'
SELECT * FROM my_lake.customers WHERE customer_id = 'CUST-0001';

.print '--- Customer CUST-0001 before update (time travel) ---'
-- Note: adjust the version number based on your actual snapshots
SELECT * FROM my_lake.customers AT (VERSION => (SELECT MAX(snapshot_id) - 1 FROM ducklake_snapshots('my_lake'))) 
WHERE customer_id = 'CUST-0001';

.print ''
.print '=== Import and Version Management Demo Complete! ==='
