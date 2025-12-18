# DuckLake Test Project

This project is for testing [DuckLake](https://ducklake.select/) - an open lakehouse format by DuckDB Labs.

## What is DuckLake?

DuckLake is an integrated data lake and catalog format that:
- Stores metadata in a SQL database (DuckDB, PostgreSQL, SQLite, MySQL, or MotherDuck)
- Stores data in Parquet files on blob storage or local filesystem
- Provides ACID transactions, time travel, schema evolution, and change data feed features

## Prerequisites

- [DuckDB](https://duckdb.org/docs/installation/) installed

## Quick Start

1. Start DuckDB:
```bash
duckdb
```

2. Install the DuckLake extension:
```sql
INSTALL ducklake;
LOAD ducklake;
```

3. Run the test scripts in the `scripts/` directory.

## Project Structure

```
ducklake-test/
├── README.md           # This file
├── data/               # Data files will be stored here
├── metadata/           # Metadata database files
└── scripts/            # SQL test scripts
    ├── 01_setup.sql    # Initial setup
    ├── 02_crud.sql     # CRUD operations
    ├── 03_time_travel.sql   # Time travel examples
    └── 04_advanced.sql      # Advanced features
```

## Real-time Data with Redpanda (Kafka)

This project includes a demo of real-time data ingestion using Redpanda and Micro-batching.

### Architecture

```mermaid
flowchart LR
    subgraph "Producer Layer"
    P[producer.py] -- "JSON Orders" --> RP
    end

    subgraph "Streaming Layer (Redpanda)"
    RP[("Redpanda Broker")]
    T[("Topic: orders")]
    RP --- T
    end

    subgraph "Ingestion Layer"
    C[consumer_sink.py]
    B[("Memory Buffer")]
    
    T -->|"Consume"| C
    C -->|"Accumulate"| B
    B -->|"Flush Batch"| DDB
    end

    subgraph "Lakehouse Layer (DuckLake)"
    DDB[("DuckDB Engine")]
    
    DL_META[("Metadata")]
    DL_DATA[("Parquet Data")]
    
    DDB -->|"Transaction"| DL_META
    DDB -->|"Write"| DL_DATA
    end
```

### Running the Demo

1. **Start Redpanda**:
   ```bash
   cd redpanda_demo
   docker-compose up -d
   ```

2. **Start Consumer (Ingestion)**:
   ```bash
   python3 consumer_sink.py
   ```

3. **Start Producer (Data Source)**:
   ```bash
   python3 producer.py
   ```

4. **Verify Data**:
   ```bash
   python3 check_data.py
   ```

## CDC with Debezium (MySQL -> DuckLake)

This demo captures real-time changes (CDC) from MySQL and streams them into DuckLake.

### Architecture

```mermaid
flowchart LR
    subgraph "Source (MySQL)"
    DB[("MySQL\n(inventory.users)")]
    Binlog[("Binlog")]
    DB --> Binlog
    end

    subgraph "CDC Layer (Debezium)"
    Connect[("Kafka Connect\nDebezium Source")]
    Redpanda[("Redpanda Broker")]
    
    Binlog -->|"Read"| Connect
    Connect -->|"CDC Events"| Redpanda
    end

    subgraph "Ingestion Layer"
    Consumer[cdc_consumer.py]
    Redpanda -->|"Consume"| Consumer
    end

    subgraph "Lakehouse Layer"
    DuckDB[("DuckDB")]
    Consumer -->|"INSERT/DELETE"| DuckDB
    
    DuckDB -->|"Write"| DL_META[("Metadata")]
    DuckDB -->|"Write"| DL_DATA[("Parquet Data")]
    end
```

### Running the CDC Demo

> **Note**: Stop any running Redpanda containers from previous demos first (`docker-compose down`).

1. **Start Infrastructure**:
   ```bash
   cd cdc_demo
   docker-compose up -d
   ```

2. **Register Connector** (Wait for containers to be ready):
   ```bash
   chmod +x register_connector.sh
   ./register_connector.sh
   ```

3. **Install Dependencies**:
   ```bash
   pip3 install pymysql
   ```

4. **Grant Permissions** (Fix for demo environment):
   ```bash
   docker exec mysql_cdc mysql -u root -pdebezium -e "GRANT INSERT, UPDATE, DELETE ON inventory.* TO 'debezium'@'%'; FLUSH PRIVILEGES;"
   ```

5. **Start Consumer** (Sync Engine):
   ```bash
   # Open a new terminal
   python3 cdc_consumer.py
   ```

6. **Start Simulator** (Generate Traffic):
   ```bash
   # Open another terminal
   python3 modify_db.py
   ```
