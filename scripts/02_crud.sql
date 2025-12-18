-- DuckLake CRUD Operations Test
-- Demonstrates Create, Read, Update, Delete operations

-- ============================================
-- CREATE: Create a sample table and insert data
-- ============================================

-- Create a products table
-- Note: DuckLake does not support PRIMARY KEY/UNIQUE constraints
CREATE TABLE IF NOT EXISTS products (
    id INTEGER,
    name VARCHAR NOT NULL,
    category VARCHAR,
    price DECIMAL(10,2),
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO products (id, name, category, price, stock) VALUES
    (1, 'Laptop', 'Electronics', 999.99, 50),
    (2, 'Keyboard', 'Electronics', 79.99, 200),
    (3, 'Mouse', 'Electronics', 29.99, 500),
    (4, 'Desk Chair', 'Furniture', 299.99, 30),
    (5, 'Monitor Stand', 'Accessories', 49.99, 100);

-- ============================================
-- READ: Query the data
-- ============================================

-- View all products
SELECT * FROM products;

-- Filter by category
SELECT name, price FROM products WHERE category = 'Electronics';

-- Aggregations
SELECT 
    category,
    COUNT(*) as count,
    AVG(price) as avg_price,
    SUM(stock) as total_stock
FROM products
GROUP BY category;

-- ============================================
-- UPDATE: Modify existing data
-- ============================================

-- Update a product's price
UPDATE products SET price = 899.99 WHERE id = 1;

-- Increase stock for all electronics
UPDATE products SET stock = stock + 10 WHERE category = 'Electronics';

-- Verify updates
SELECT * FROM products;

-- ============================================
-- DELETE: Remove data
-- ============================================

-- Delete a product by ID
DELETE FROM products WHERE id = 5;

-- Verify deletion
SELECT * FROM products;
