# TPC-H PostgreSQL Extension

The `tpch` extension implements the data generator and queries for the
[TPC-H benchmark](https://www.tpc.org/tpch/) Version 3.0.1, and provides an easy way to run all 22 queries in order.

## Install

`make install` compiles the TPC-H dbgen tools from source, so you need a C toolchain:

```bash
# Debian/Ubuntu
sudo apt-get install build-essential

# RHEL/Rocky/CentOS/Fedora
sudo dnf install gcc make
```

Then:

```bash
cd pg_tpch
make install
```

> **Note**: The TPC-H reference data (`ref_data/`) and specification files are excluded
> from the git repository to keep it small. They are not needed to build or run the
> extension. If you need them for result validation, re-extract from the official
> TPC-H zip:
> ```bash
> unzip /path/to/TPC-H-TOOL.zip 'TPC-H V3.0.1/ref_data/*' 'TPC-H V3.0.1/specification.*'
> mv 'TPC-H V3.0.1/ref_data' 'TPC-H V3.0.1/specification.'* TPC-H-V3.0.1/
> rm -r 'TPC-H V3.0.1'
> ```

## Quick Start

### One-shot

```sql
CREATE EXTENSION tpch;
SELECT tpch.config('data_dir', '/data/tpch_tmp');  -- set data dir (default: /tmp/tpch_data)
CALL tpch.run(1, 8);  -- 1 scale factor (GB), 8 parallel workers
```

`run()` executes the full pipeline: schema → data generation → load → query generation → benchmark.

### Step by step

```sql
CREATE EXTENSION tpch;
SELECT tpch.gen_schema();         -- 1. create 8 TPC-H tables
SELECT tpch.gen_data(1, 8);       -- 2. generate SF-1 (~1 GB) .tbl files, 8 parallel workers
SELECT tpch.load_data(8);         -- 3. load .tbl files into tables, 8 parallel workers
SELECT tpch.gen_query(1);         -- 4. generate 22 queries for SF-1
SELECT tpch.bench();              -- 5. run all 22 queries
SELECT tpch.clean_data();         -- 6. (optional) delete .tbl files to free disk space
```

Check the latest results:

```sql
SELECT * FROM tpch.bench_summary;
```

Built and tested on **PostgreSQL 19devel**. Older versions should also work. If not, please create an issue.

## Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `tpch.config(key)` | TEXT | Get config value |
| `tpch.config(key, value)` | TEXT | Set config value |
| `tpch.info()` | TABLE | Show all resolved paths and scale factor |
| `tpch.run(scale, parallel=1)` | — | Full pipeline: gen_schema → gen_data → load_data → gen_query → bench |
| `tpch.gen_schema()` | TEXT | Create 8 TPC-H tables under `tpch` schema |
| `tpch.gen_data(scale, parallel=1)` | TEXT | Generate .tbl files via dbgen |
| `tpch.load_data(workers=4)` | TEXT | Load .tbl files into tables and analyze |
| `tpch.clean_data()` | TEXT | Delete .tbl files from data_dir to free disk space |
| `tpch.gen_query(scale)` | TEXT | Generate 22 queries for the given scale factor |
| `tpch.show(qid)` | TEXT | Return query text |
| `tpch.exec(qid)` | TEXT | Execute one query, save result to `tpch.bench_results` |
| `tpch.bench(mode)` | TEXT | Run or explain all 22 queries, update `bench_summary` |
| `tpch.explain(qid, opts)` | SETOF TEXT | EXPLAIN a single query |

### run(scale, parallel)

Runs the complete benchmark pipeline in one call.

```sql
CALL tpch.run();        -- SF=1, single-threaded
CALL tpch.run(100, 8);  -- SF=100, 8 dbgen workers
```

`parallel` controls both data generation (dbgen workers) and loading (table-COPY workers, capped
internally at `LEAST(parallel, 8)` since TPC-H has only 8 tables).

### show(qid)

Show the query 6's text.
```sql
SELECT tpch.show(6);
                     show
----------------------------------------------
 select
         sum(l_extendedprice * l_discount) as revenue
 from
         lineitem
 where
         l_shipdate >= date '1994-01-01'
         and l_shipdate < date '1994-01-01' + interval '1' year
         and l_discount between 0.06 - 0.01 and 0.06 + 0.01
         and l_quantity < 24;
```

### explain(qid, opts)

See the plan of query 6.
```sql
SELECT * FROM tpch.explain(6, 'COSTS OFF');
                                                                                                    explain
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate
   ->  Seq Scan on lineitem
         Filter: ((l_shipdate >= '1994-01-01'::date) AND (l_shipdate < '1995-01-01 00:00:00'::timestamp without time zone) AND (l_discount >= 0.05) AND (l_discount <= 0.07) AND (l_quantity < '24'::numeric))
(3 rows)
```

### bench(mode)

Run all 22 queries and record results.

```sql
SELECT tpch.bench();                               -- execute all 22 queries
SELECT tpch.bench('EXPLAIN');                      -- explain all 22 queries
SELECT tpch.bench('EXPLAIN (ANALYZE, COSTS OFF)'); -- explain with options
```

Output saved to `results_dir`:
- Per-query: `query1.out` ... `query22.out` (or `query1_explain.out` ... `query22_explain.out`)
- Summary: `summary.csv` — query_id, status, duration_ms, rows_returned

`tpch.bench_summary` is updated after each run with the latest results.

## Where Things Are Stored

### Tables (all under `tpch` schema)

| Table | Populated by | Description |
|-------|-------------|-------------|
| `tpch.config` | `CREATE EXTENSION` | Configuration |
| `tpch.query` | `gen_query()` | 22 generated query texts |
| `tpch.bench_summary` | `bench()` | Latest run: query_id, status, duration_ms, rows_returned (updated each run) |
| `tpch.bench_results` | `exec()` / `bench()` | All historical results (appended each run) |
| 8 data tables | `gen_schema()` + `gen_data()` | `lineitem`, `orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region` |

### Directories (auto-detected under extension install path)

| Directory | Contents |
|-----------|----------|
| `query_dir` | `query1.sql` ... `query22.sql` from `gen_query()` |
| `results_dir` | Per-query `.out` files and `summary.csv` from `bench()` |
| `data_dir` | Temporary `.tbl` files from `gen_data()` (default: `/tmp/tpch_data`) |

Check all resolved paths:

```sql
SELECT * FROM tpch.info();
```

## Configuration

Everything works out of the box. All directories except `data_dir` are auto-detected under the extension install path. Optional overrides:

```sql
SELECT tpch.config('data_dir', '/data/tpch');
SELECT tpch.config('results_dir', '/data/results');
SELECT tpch.config('query_dir', '/data/queries');
```

> **Disk space warning:** `gen_data()` writes raw `.tbl` files to `data_dir` before loading them into
> PostgreSQL. The `.tbl` files are roughly the same size as the loaded data (~1 GB per scale
> factor). Make sure `data_dir` has enough free space — at least **2× the scale factor in GB** to
> account for both the `.tbl` files and the database storage. The default `data_dir` is
> `/tmp/tpch_data`, which may be too small for large scale factors. Set it to a partition with
> sufficient space before running `gen_data()`:
>
> ```sql
> SELECT tpch.config('data_dir', '/data/tpch_tmp');
> SELECT tpch.gen_data(100, 8);  -- SF=100 needs ~100 GB in data_dir
> ```

## PostgreSQL Compatibility Fixes

`gen_query()` automatically patches raw `qgen` output:

1. **Interval precision** — `interval '90' day (3)` → `interval '90' day` (strip unsupported precision qualifier)
2. **Standalone LIMIT** — relocates `LIMIT N` from separate line into the query body
3. **Control directives** — strips qgen directives (`:x`, `:o`, `:n`, etc.) and carriage returns
4. **Query 15 (Top Supplier)** — `CREATE VIEW` / `DROP VIEW` handled as DDL in explain and bench modes

## License

PostgreSQL License. See LICENSE file.

> **Disclaimer**: TPC-H is a trademark of the Transaction Processing Performance Council (TPC). This extension is **not** an official TPC benchmark implementation. Any results produced should be referred to as "derived from" or "based on" TPC-H. See NOTICE file.
