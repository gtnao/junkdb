![DALL·E 2024-01-07 06 19 02 - Create a 3D, simplified version of the previous image representing 'ToyDB', a relational database management system, without any text or labels on the (2)](https://github.com/gtnao0219/toydb/assets/25474324/0926e663-95e7-4fe3-a938-f28bbd05dd69)

# Overview

toydb is a Relational Database Management System written in Rust for my own study purposes.

# Feature

- Rust-based
- Basic RDBMS Operations
  - DML
    - CRUD (SELECT, INSERT, DELETE, and UPDATE)
    - Filtering (WHERE)
    - Join
      - NLJ (Nested Loop Join)
    - Aggregation (GROUP BY and HAVING)
    - Sorting (ORDER BY AND LIMIT)
  - DDL
    - CREATE TABLE
- Transactions:
  - MVCC (Multi-Version Concurrency Control)
    - Snapshot Isolation
  - IsolationLevel
    - READ COMMITTED
    - REPEATABLE READ
  - Vacuum: clean up old data versions.
  - Lock
    - Row-level Exclusive Locking: prevent dirty write.
    - Deadlock Detection
  - Recovery (ARIES)
- Indexing
  - B+ Tree
- BufferPool
  - LRU
- Optimizer
  - Rule-Based
- Basic Data Type
  - INT, VARCHAR, BOOLEAN
  - NULL
- Basic SQL Parser
- CLI
