# Social Media DBMS (SQL-Heavy Project)

This project implements your proposal as a **working relational DBMS prototype** focused on:
- strong schema design (PK/FK/check constraints)
- heavy SQL query logic (joins, CTEs, recursive CTEs, windows, cohort analysis)
- realistic social-media entities (`users`, `posts`, `likes`, `comments`, `follows`, `user_activity`)
- CPU query benchmarking and an optional GPU-ready aggregation comparison path

## Project Structure

- `sql/schema.sql` - relational schema + constraints
- `sql/triggers.sql` - business logic triggers (activity logging, denormalized counters)
- `sql/indexes.sql` - indexing strategy for query performance
- `sql/views.sql` - reusable analytics views
- `sql/analytical_queries.sql` - SQL query bank for report/demo
- `src/db_builder.py` - builds and seeds the database with synthetic large-scale data
- `src/query_runner.py` - executes advanced analytical SQL queries
- `src/benchmark.py` - query benchmark + SQL vs vector backend aggregation
- `data/social_media.db` - generated SQLite database (created after build)

## Quick Start

From `/Users/devansh/dbmsproject`:

```bash
python3 src/db_builder.py --rebuild
```

This creates and seeds `data/social_media.db` with defaults:
- 3000 users
- 18000 posts
- 90000 likes
- 45000 comments
- 22000 follows
- login activity + trigger-generated activity records

Run all analytical queries:

```bash
python3 src/query_runner.py --query all --limit 10
```

List available query names:

```bash
python3 src/query_runner.py --list
```

Run a single query (example):

```bash
python3 src/query_runner.py --query comment_thread --post-id 120 --limit 20
```

Inspect execution plan:

```bash
python3 src/query_runner.py --query trending_tags --explain
```

Run benchmarks:

```bash
python3 src/benchmark.py --iterations 4 --days 30
```

## SQL Highlights (for DBMS evaluation)

The query workload intentionally demonstrates core DBMS concepts:
- multi-table joins (`posts` + `likes` + `comments` + `tags`)
- aggregate analytics (`SUM`, `COUNT`, weighted scores)
- window functions (`RANK`, `DENSE_RANK`, rolling average)
- recursive CTE for nested comment threads
- recommendation-style collaborative filtering query
- cohort retention analysis over monthly buckets
- trigger-driven consistency (`like_count`, `comment_count`, activity logs)

## GPU/Acceleration Note

The benchmark includes:
- SQL baseline: `GROUP BY` aggregation in SQLite
- vector backend: `numpy` (default), or `cupy` if available

If CuPy is installed and CUDA is available, run:

```bash
python3 src/benchmark.py --engine cupy
```

Otherwise `--engine auto` falls back to NumPy.

## Suggested Demo Flow for Presentation

1. Build database with a larger dataset (`--users`, `--posts`, `--likes` flags).
2. Run `query_runner.py --query all` and show timings.
3. Run one query with `--explain` to discuss indexing and plans.
4. Run `benchmark.py` to compare SQL aggregation and vector acceleration.
5. Explain how triggers enforce consistency and activity capture.

## Useful Build Variants

Bigger dataset for stress testing:

```bash
python3 src/db_builder.py --rebuild --users 6000 --posts 50000 --likes 260000 --comments 140000 --follows 60000
```

Use deterministic generation:

```bash
python3 src/db_builder.py --rebuild --seed 123
```
