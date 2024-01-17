[![Test](https://github.com/gtnao0219/toydb/actions/workflows/test.yml/badge.svg)](https://github.com/gtnao0219/toydb/actions/workflows/test.yml)

![DALL·E 2024-01-07 06 19 02 - Create a 3D, simplified version of the previous image representing 'ToyDB', a relational database management system, without any text or labels on the (2)](https://github.com/gtnao0219/toydb/assets/25474324/0926e663-95e7-4fe3-a938-f28bbd05dd69)

# Overview

**toydb** is a Relational Database Management System written in Rust for my own study purposes.

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
- [ ] Data Type
  - [x] NUMBER(INT, UNSIGNED INT, BIGINT, UNSIGNED BIGINT), VARCHAR, BOOLEAN
  - [x] NULL
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
