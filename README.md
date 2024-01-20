[![Test](https://github.com/gtnao0219/junkdb/actions/workflows/test.yml/badge.svg)](https://github.com/gtnao0219/junkdb/actions/workflows/test.yml)

![junkdb (1)](https://github.com/gtnao0219/toydb/assets/25474324/d2025f88-06f4-4ed8-a3f3-39b910163de5)

# Overview

**junkdb** is a Relational Database Management System written in Rust for my own study purposes.

# Screenshot

The following shows REPEATABLE READ implemented via snapshot isolation.

![mvcc](https://github.com/gtnao0219/toydb/assets/25474324/74254571-b03c-45e6-b515-f5962bb27f76)

# Feature

- Rust-based
- Basic RDBMS Operations
  - DML
    - [x] CRUD (SELECT, INSERT, DELETE, and UPDATE)
    - [x] Filtering (WHERE)
    - [x] Subquery
    - [x] Join
      - [x] Nested Loop Join
        - [x] Inner Join
        - [x] Left Join
    - [x] Aggregation (GROUP BY and HAVING)
      - [x] Functions (COUNT, SUM, MAX, MIN, AVG)
    - [x] Sorting (ORDER BY and LIMIT)
  - DDL
    - [x] CREATE TABLE
- Transactions:
  - [x] MVCC (Multi-Version Concurrency Control)
    - [x] Snapshot Isolation
  - [x] Isolation Level
    - [x] READ COMMITTED
    - [x] REPEATABLE READ
  - [ ] Vacuum: clean up old data versions.
  - [ ] Lock
    - [x] Row-level Exclusive Locking: prevent dirty write.
    - [ ] Deadlock Detection
  - [x] Recovery (ARIES-based)
    - [ ] Checkpoint
- [ ] Indexing
  - [ ] B+ Tree
- [x] Buffer Pool
  - [x] LRU
- [ ] Optimizer
  - [ ] Rule-based
- [x] Data Types
  - [x] INTEGER, VARCHAR, BOOLEAN
  - [x] NULL
- [x] Operators
  - [x] Binary (=, <>, <, >, <=, >=, +, -, \*, /, %, AND, OR)
  - [x] UNARY (-, NOT)
  - [x] IS NULL, IS NOT NULL
- [x] Basic SQL Parser
- [x] CLI

# Usage

## Server

```command
cargo run server --init
```

## Client

```command
cargo run client
```
