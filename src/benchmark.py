#!/usr/bin/env python3
"""Benchmark SQL workloads and optional GPU-ready aggregation paths."""

from __future__ import annotations

import argparse
import sqlite3
import statistics
import time
from pathlib import Path

import numpy as np

from query_runner import DEFAULT_DB_PATH, choose_default_post, choose_default_user, query_catalog

try:
    import cupy as cp  # type: ignore
except Exception:
    cp = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark query workloads for the DBMS project.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite DB file.")
    parser.add_argument("--iterations", type=int, default=4, help="Iterations per query.")
    parser.add_argument("--limit", type=int, default=25, help="Row limit for benchmark queries.")
    parser.add_argument("--days", type=int, default=30, help="Trending window (days).")
    parser.add_argument(
        "--engine",
        choices=("auto", "numpy", "cupy"),
        default="auto",
        help="Backend for vector aggregation benchmark.",
    )
    return parser.parse_args()


def run_sql_benchmark(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    catalog = query_catalog()
    selected_names = [
        "top_influencers",
        "trending_topics",
        "trending_tags",
        "activity_spikes",
        "cohort_retention",
    ]

    # stabilize user/post-specific query params if these are used later
    default_user = choose_default_user(conn)
    default_post = choose_default_post(conn)

    run_args = argparse.Namespace(
        limit=args.limit,
        days=args.days,
        user_id=default_user,
        post_id=default_post,
    )

    print("\n=== SQL Query Benchmark ===")
    results: list[tuple[str, float, float]] = []

    for name in selected_names:
        query_def = catalog[name]
        params = query_def.params_factory(run_args, conn)
        sql = query_def.sql

        samples_ms = []
        # warm up
        conn.execute(sql, params).fetchall()

        for _ in range(args.iterations):
            t0 = time.perf_counter()
            conn.execute(sql, params).fetchall()
            samples_ms.append((time.perf_counter() - t0) * 1000)

        mean_ms = statistics.fmean(samples_ms)
        stdev_ms = statistics.pstdev(samples_ms) if len(samples_ms) > 1 else 0.0
        results.append((name, mean_ms, stdev_ms))

    print("query                | mean_ms | stdev_ms")
    print("---------------------+---------+---------")
    for name, mean_ms, stdev_ms in results:
        print(f"{name:20s} | {mean_ms:7.2f} | {stdev_ms:7.2f}")


def load_like_user_ids(conn: sqlite3.Connection) -> np.ndarray:
    rows = conn.execute("SELECT user_id FROM likes ORDER BY like_id").fetchall()
    return np.array([row[0] for row in rows], dtype=np.int32)


def vector_backend(engine: str):
    if engine == "numpy":
        return "numpy", np
    if engine == "cupy":
        if cp is None:
            raise RuntimeError("CuPy not available; install cupy to use --engine cupy")
        return "cupy", cp

    # auto mode
    if cp is not None:
        return "cupy", cp
    return "numpy", np


def benchmark_like_aggregation(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    print("\n=== Aggregation Benchmark (likes per user) ===")

    # SQL baseline
    sql_samples = []
    conn.execute("SELECT user_id, COUNT(*) FROM likes GROUP BY user_id").fetchall()  # warm up
    for _ in range(args.iterations):
        t0 = time.perf_counter()
        sql_rows = conn.execute("SELECT user_id, COUNT(*) FROM likes GROUP BY user_id").fetchall()
        sql_samples.append((time.perf_counter() - t0) * 1000)

    sql_mean = statistics.fmean(sql_samples)

    # Vector backend (NumPy or CuPy)
    backend_name, xp = vector_backend(args.engine)
    user_ids = load_like_user_ids(conn)

    if backend_name == "cupy":
        user_ids_device = xp.asarray(user_ids)
        xp.cuda.Stream.null.synchronize()
        vector_samples = []
        for _ in range(args.iterations):
            t0 = time.perf_counter()
            counts = xp.bincount(user_ids_device)
            xp.cuda.Stream.null.synchronize()
            _ = int(xp.argmax(counts).item())
            vector_samples.append((time.perf_counter() - t0) * 1000)
    else:
        vector_samples = []
        for _ in range(args.iterations):
            t0 = time.perf_counter()
            counts = xp.bincount(user_ids)
            _ = int(xp.argmax(counts).item())
            vector_samples.append((time.perf_counter() - t0) * 1000)

    vector_mean = statistics.fmean(vector_samples)
    speedup = sql_mean / vector_mean if vector_mean > 0 else float("inf")

    print(f"SQL GROUP BY avg:       {sql_mean:.2f} ms")
    print(f"{backend_name.upper()} bincount avg: {vector_mean:.2f} ms")
    print(f"Observed speedup:       {speedup:.2f}x")
    print("(Vector benchmark focuses on a parallelizable aggregation kernel.)")

    # keep a simple correctness check
    sql_top = max(sql_rows, key=lambda x: x[1]) if sql_rows else None
    if sql_top:
        print(f"Most active liker by SQL: user_id={sql_top[0]}, likes={sql_top[1]}")


def main() -> None:
    args = parse_args()
    if not args.db.exists():
        raise FileNotFoundError(f"Database file not found: {args.db}")

    conn = sqlite3.connect(str(args.db))
    try:
        run_sql_benchmark(conn, args)
        benchmark_like_aggregation(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
