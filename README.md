# niwid-db

[niwid-db](https://github.com/MohamedAbdeen21/niwid-db) is a toy database management system built from scratch, designed to explore fundamental database concepts, including memory and disk management, ACID properties, basic transaction handling, indexing, and query execution.

## Features

- **ACID Compliance via Shadow Paging**: Ensures **Atomicity, Consistency, Isolation (read-committed),** and **Durability** by implementing shadow paging, maintaining a stable, crash-resistant state. Shadow paging allows a single-writer approach for simplified concurrency control.

- **Transaction Management**: Supports **commits** and **rollbacks** to execute or discard changes within a transaction block, ensuring transactional atomicity and isolation.

- **Custom Execution Engine**: A simple query engine that processes SQL statements and performs basic DDL, TCL, and DML operations. Also supports "EXPLAIN" and "EXPLAIN ANALYZE". No physical plans or optimizations yet.

- **SQL Parsing**: Leverages the `sqlparser-rs` crate for SQL syntax parsing, with custom implementations for query interpretation and execution. This is the only part of the system that is not from scratch.

- **B+ Tree Indexing**: Implements a B+ Tree index to optimize data retrieval and ensure uniqueness, enabling efficient lookups and range queries for unique columns.

- **Constraints**: Supports `NOT NULL` and `UNIQUE` constraints to enforce data integrity, though primary keys and foreign keys (referential integrity) are not supported.

- **Joins**: Enables basic nested loop join operations between tables, allowing for relational queries.

## Getting Started

### Prerequisites

- **Rust**: niwid-db is written in Rust.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/MohamedAbdeen21/niwid-db.git
   cd niwid-db
   ```

2. Run the server:
   ```bash
   cargo run --release
   ```

3. Connect to the server from a different terminal:
   ```bash
   nc localhost 8080
   ```

### Usage

niwid-db can execute SQL queries for managing tables, running transactions, and executing basic SQL commands.

Example:
```sql
CREATE TABLE users (
    id INT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
```

You can also use transactions to batch operations:
```sql
BEGIN;
UPDATE users SET email = 'alice_new@example.com' WHERE id = 1;
ROLLBACK; -- Discards the update
```

## Contributing

Contributions are always welcome! Feel free to submit issues or pull requests.


And yes, this README is written by ChatGPT.
