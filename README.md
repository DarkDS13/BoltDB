<div align="center">
  <h1>⚡ BoltDB</h1>
  <p><b>High-Performance GPU-Accelerated Analytics DBMS</b></p>
  <p><i>A prototype relational database mimicking scaling social media entities, featuring advanced SQL querying, Apple Metal GPU acceleration, and a stunning real-time analytics dashboard.</i></p>
  
  ---
</div>

## 🚀 Overview

BoltDB is a robust relational DBMS prototype built to demonstrate state-of-the-art database management, data synthesis, and query acceleration. It features a completely generated social media schema with millions of potential relationships, tested against advanced analytical queries (Recursive CTEs, Windowing, Aggregations), and benchmarked using both standard CPU operations and vector-accelerated Apple Metal (MLX) processing.

### 🌟 Key Features
- **Rigorous Schema Design:** Built on SQLite with strong PK/FK relationships and check constraints.
- **Advanced Analytical SQL:** Complex recursive querying, multi-table joins, cohort analysis, and rolling window aggregations.
- **Trigger-Driven Consistency:** Real-time business logic triggers for automated activity logging and denormalized counting.
- **Hardware Acceleration:** Native fallback aggregations utilizing NumPy and Apple Metal GPU via `mlx`.
- **Real-time Glassmorphism Dashboard:** A stunning, premium web dashboard serving live database visualizations and benchmarking scale performance.

<br>

## 📁 Architecture & Structure

```text
├── data/
│   └── social_media.db          # Auto-generated SQLite database
├── sql/
│   ├── schema.sql               # Relational data models & constraints
│   ├── indexes.sql              # B-Tree indexing strategy for performance
│   ├── triggers.sql             # Business logic & consistency triggers
│   ├── views.sql                # Reusable analytical views
│   └── analytical_queries.sql   # SQL query bank for reporting
├── src/
│   ├── db_builder.py            # Synthesizes large-scale relational data
│   ├── query_runner.py          # Executes complex analytics
│   ├── benchmark.py             # Advanced benching (SQL vs NumPy vs MLX)
│   └── app.py                   # High-performance Flask backend for the Dashboard
└── frontend/
    └── index.html               # Sleek, Thematic UI Dashboard
```

<br>

## 🏎️ Quick Start

**1. Generate the Database**
Build and seed the database with synthetic social interactions:
```bash
python3 src/db_builder.py --rebuild
```
*Tip: Scale the dataset up massively for stress-testing GPU acceleration:*
```bash
python3 src/db_builder.py --rebuild --users 6000 --posts 50000 --likes 260000 
```

**2. Launch the Dashboard (Recommended)**
Start the Flask analytics server to view live benchmarks and visualizations:
```bash
cd src
python3 app.py
```
*Navigate to `http://localhost:5050` in your browser to view the BoltDB frontend.*

**3. Run the Scale Benchmark**
Compare CPU vs Vector GPU aggregation directly from the CLI:
```bash
python3 src/benchmark.py --iterations 4 --days 30
```

**4. Execute Analytics via CLI**
Run all complex SQL analytical queries programmatically:
```bash
python3 src/query_runner.py --query all --limit 10
```

<br>

## 🔎 SQL Evaluation Highlights
The analytical workload demonstrates advanced DBMS foundations:
- **Multi-table Joins:** Deep relations tracking (`posts` → `likes` → `comments` → `tags`).
- **Window Functions:** Ranking influencers utilizing `DENSE_RANK` and partitioning rolling averages.
- **Recursive CTEs:** Constructing nested n-depth comment threads dynamically.
- **Cohort Retention:** Segmenting user lifecycles over monthly activity buckets.
- **Collaborative Filtering:** Recommendation-style logic for complex join scenarios.

<br>

## ⚡ GPU / Hardware Acceleration
We push the limits of lightweight databases by vectorizing aggregations:
- **Baseline:** Standard SQLite `GROUP BY` Operations (CPU)
- **Vector CPU:** Memory-mapped `numpy` aggregations.
- **Apple Silicon Pipeline:** Specialized `mlx` core aggregation executing directly on Apple Metal GPUs for massive speedups at extreme row counts.


<div align="center">
  <br>
  <i>Lightning Fast. Relational Integrity. BoltDB.</i>
</div>

<br>

## 👥 Creators
Built with passion by:
- **Adithya Shyam**
- **Dev Patel**
- **Devansh Shah**
