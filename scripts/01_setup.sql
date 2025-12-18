-- DuckLake Setup Script
-- Run this first to configure your DuckLake environment

-- Install and load the DuckLake extension
INSTALL ducklake;
LOAD ducklake;

-- Attach a DuckLake database with local storage
-- Metadata stored in: ../metadata/my_lake.ducklake
-- Data files stored in: ../data/
ATTACH 'ducklake:../metadata/my_lake.ducklake' AS my_lake (DATA_PATH '../data/');

-- Switch to the DuckLake database
USE my_lake;

-- Verify the connection
SELECT 'DuckLake successfully attached!' AS status;
