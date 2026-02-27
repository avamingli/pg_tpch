CREATE EXTENSION tpch;
SELECT tpch.gen_schema();
SELECT count(*) FROM pg_tables WHERE schemaname = 'tpch'
  AND tablename NOT IN ('config', 'query', 'bench_results', 'bench_summary');
SELECT tablename FROM pg_tables WHERE schemaname = 'tpch'
  AND tablename NOT IN ('config', 'query', 'bench_results', 'bench_summary')
  ORDER BY tablename;
SHOW search_path;
