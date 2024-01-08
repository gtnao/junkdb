[![Test](https://github.com/gtnao0219/toydb/actions/workflows/test.yml/badge.svg)](https://github.com/gtnao0219/toydb/actions/workflows/test.yml)

![DALL·E 2024-01-07 06 19 02 - Create a 3D, simplified version of the previous image representing 'ToyDB', a relational database management system, without any text or labels on the (2)](https://github.com/gtnao0219/toydb/assets/25474324/0926e663-95e7-4fe3-a938-f28bbd05dd69)


# Overview

toydb is a Relational Database Management System written in Rust for my own study purposes.

# Screenshot

# Basic SQL

![toydb1](https://github.com/gtnao0219/toydb/assets/25474324/627317ca-76ca-4803-9dc6-639abac2995a)

# Feature

- Rust-based
- Basic RDBMS Operations
  - DML
    - [x] CRUD (SELECT, INSERT, DELETE, and UPDATE)
    - [x] Filtering (WHERE)
    - [ ] Join
      - [ ] Nested Loop Join
    - [ ] Aggregation (GROUP BY and HAVING)
    - [ ] Sorting (ORDER BY and LIMIT)
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
  - [ ] Recovery (ARIES-based)
- [ ] Indexing
  - [ ] B+ Tree
- [x] Buffer Pool
  - [x] LRU
- [ ] Optimizer
  - [ ] Rule-based
- [ ] Basic Data Type
  - [x] INT, VARCHAR, BOOLEAN
  - [ ] NULL
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
