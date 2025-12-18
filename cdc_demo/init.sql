-- Initialize database
CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    role VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Seed data
INSERT INTO users (name, email, role) VALUES 
('Alice', 'alice@example.com', 'admin'),
('Bob', 'bob@example.com', 'user'),
('Charlie', 'charlie@example.com', 'user');

-- Create a dedicated user for Debezium (best practice)
CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
