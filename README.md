# niwid-db

[niwid-db](https://github.com/MohamedAbdeen21/niwid-db) is a toy database management system built from scratch, designed to explore fundamental database concepts, including memory and disk management, ACID properties, basic transaction handling, indexing, and query execution.

### Table of Contents

1. [Features](#features)
2. [Getting Started](#getting-started)
3. [Examples](#examples)
4. [Some Implementation Details](#some-implementation-details)
5. [Contributing](#contributing)

## Features

- **ACID Compliance via Shadow Paging**: Ensures **Atomicity, Consistency, Isolation (read-committed),** and **Durability** by implementing shadow paging, maintaining a stable, crash-resistant state. Shadow paging allows a table to only have single-writer for simplified concurrency control.

- **Transaction Management**: Supports **commits** and **rollbacks** to execute or discard changes within a transaction block to ensure transactional atomicity and isolation.

- **Custom Execution Engine**: A simple query engine that processes SQL statements, validates the query and datatypes, and performs basic DDL, TCL, and DML operations. Also supports `EXPLAIN` and `EXPLAIN ANALYZE`. Unoptimized logical plans are executed directly, no physical plan generation or optimizations yet.

- **SQL Parsing**: The only part not written from scratch. Leverages the `sqlparser-rs` crate for SQL syntax parsing.

- **B+ Tree Indexing**: Implements a B+ Tree index to ensure uniqueness, enabling efficient lookups and range queries for unique columns. Since there is no optimizer yet, you can force an index lookup using the `PREWHERE` clause. Check out `index.slt` for more examples.

- **Constraints**: Supports `NOT NULL` and `UNIQUE` constraints to enforce data integrity, although primary keys and foreign keys (referential integrity) are not supported.

- **Joins**: Performs basic nested loop join operations between tables, allowing for relational queries.

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

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
```

You can also use transactions to batch operations:
```sql
BEGIN;
UPDATE users SET email = 'bob@example.com' WHERE id = 1;
ROLLBACK; -- Discards the update
```

## Examples

For more examples, check out [the sqllogictests](https://github.com/MohamedAbdeen21/niwid-db/tree/main/src/sqllogictest/slt_files). If you're not familiar with slt files, check out [how to read sqllogictests](https://github.com/MohamedAbdeen21/niwid-db/blob/main/src/sqllogictest/README.md)

## Some implementation details

- Slotted pages, with string indirection and a B+ Tree Index.
- LRU page eviction policy
- Shadow-Paging for ACID. No MVCC, OCC, 2PL, or WAL.
- Simple query engine, directly execute the raw logicial plan. No optimizer, or physical plan builder. The project focused on
exploring the internals of the database, not the query engine.
- Catalog as a read-only Table, check it out using `SELECT * FROM __CATALOG__`;
- Using `sqlparser.rs`, this is the only part not written from scratch. I wrote my fair share of parsers (and I did some contributing to sqlparser-rs), but again, this was not the main focus of the project.

## Contributing

Contributions are always welcome! Feel free to submit issues or pull requests.
