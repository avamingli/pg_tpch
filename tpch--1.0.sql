-- TPC-H PostgreSQL Extension v1.0
-- Config-driven, dynamic query generation, clean function names

-- =============================================================================
-- Config table
-- =============================================================================
CREATE TABLE tpch.config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO tpch.config (key, value) VALUES
    ('tpch_dir', ''),
    ('data_dir', '/tmp/tpch_data'),
    ('query_dir', ''),
    ('results_dir', '');

-- =============================================================================
-- Queries table — populated by gen_query()
-- =============================================================================
CREATE TABLE tpch.query (
    query_id   INTEGER PRIMARY KEY,
    query_text TEXT NOT NULL
);

-- =============================================================================
-- Benchmark results table (historical, appended each run)
-- =============================================================================
CREATE TABLE tpch.bench_results (
    id            SERIAL PRIMARY KEY,
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_id      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL
);

-- =============================================================================
-- Benchmark summary table (latest run only, updated each bench())
-- =============================================================================
CREATE TABLE tpch.bench_summary (
    query_id      INTEGER PRIMARY KEY,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL,
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- _get_config(key) — read config value, raise if not set
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch._get_config(cfg_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpch.config WHERE key = cfg_key;
    IF _val IS NULL OR _val = '' THEN
        RAISE EXCEPTION 'tpch.config key "%" is not set. Run: UPDATE tpch.config SET value = ''...'' WHERE key = ''%''',
            cfg_key, cfg_key;
    END IF;
    RETURN _val;
END;
$func$;

-- =============================================================================
-- _resolve_dir(cfg_key, default_subdir) — config override or auto-detect
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch._resolve_dir(cfg_key TEXT, default_subdir TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
    _sharedir TEXT;
BEGIN
    SELECT value INTO _val FROM tpch.config WHERE key = cfg_key;
    IF _val IS NOT NULL AND _val <> '' THEN
        RETURN _val;
    END IF;
    SELECT setting INTO _sharedir FROM pg_config() WHERE name = 'SHAREDIR';
    RETURN _sharedir || '/extension/' || default_subdir;
END;
$func$;

-- =============================================================================
-- info() — show resolved configuration
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.info()
RETURNS TABLE(key TEXT, value TEXT)
LANGUAGE plpgsql
AS $func$
BEGIN
    RETURN QUERY
    SELECT 'tpch_dir'::TEXT,    tpch._resolve_dir('tpch_dir', 'tpch_dbgen')
    UNION ALL
    SELECT 'data_dir',
           COALESCE(NULLIF((SELECT c.value FROM tpch.config c WHERE c.key = 'data_dir'), ''), '/tmp/tpch_data')
    UNION ALL
    SELECT 'query_dir', tpch._resolve_dir('query_dir', 'tpch_query')
    UNION ALL
    SELECT 'results_dir', tpch._resolve_dir('results_dir', 'tpch_results')
    UNION ALL
    SELECT 'scale_factor',
           COALESCE((SELECT c.value FROM tpch.config c WHERE c.key = 'scale_factor'), '(not set)');
END;
$func$;

-- =============================================================================
-- _fix_query(qid, sql) — apply PostgreSQL compatibility fixes to qgen output
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch._fix_query(qid INTEGER, raw_sql TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT := raw_sql;
    _limit_val TEXT;
    _last_semi INTEGER;
BEGIN
    -- Strip "-- using default substitutions" header
    _sql := regexp_replace(_sql, '(?m)^--\s*using default substitutions\s*$', '', 'g');

    -- Strip qgen directives that may remain in output
    _sql := regexp_replace(_sql, '(?m)^\s*:[xobeq]\s*$', '', 'g');
    _sql := regexp_replace(_sql, '(?m)^\s*:n\s+-?\d+\s*$', '', 'g');

    -- Fix: interval precision "(3)" not supported in PostgreSQL interval literals
    -- e.g. interval '90' day (3) → interval '90' day
    _sql := regexp_replace(_sql, '(interval\s+''[^'']*''\s+\w+)\s*\(\d+\)', '\1', 'gi');

    -- Handle standalone LIMIT lines produced by qgen
    -- "LIMIT -1" means no limit — just remove it
    _sql := regexp_replace(_sql, '(?m)^\s*LIMIT\s+-1\s*$', '', 'gi');

    -- "LIMIT N" (N > 0) needs to be moved before the last semicolon
    IF _sql ~ '(?m)^\s*LIMIT\s+[1-9]' THEN
        _limit_val := (regexp_match(_sql, '(?m)^\s*(LIMIT\s+\d+)', 'i'))[1];
        IF _limit_val IS NOT NULL THEN
            -- Remove the standalone LIMIT line
            _sql := regexp_replace(_sql, '(?m)^\s*LIMIT\s+\d+\s*$', '', 'gi');
            -- Find the last semicolon and insert LIMIT before it
            _sql := btrim(_sql, E' \t\n\r');
            _last_semi := length(_sql) - position(';' in reverse(_sql)) + 1;
            IF _last_semi > 0 AND _last_semi <= length(_sql) THEN
                _sql := left(_sql, _last_semi - 1) || E'\n' || _limit_val || right(_sql, length(_sql) - _last_semi + 1);
            END IF;
        END IF;
    END IF;

    -- Remove leading/trailing whitespace
    _sql := btrim(_sql, E' \t\n\r');

    RETURN _sql;
END;
$func$;

-- =============================================================================
-- gen_schema() — create 8 TPC-H tables (embedded DDL)
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.gen_schema()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tbl TEXT;
    _tables TEXT[] := ARRAY[
        'nation','region','part','supplier',
        'partsupp','customer','orders','lineitem'
    ];
BEGIN
    SET LOCAL client_min_messages = warning;
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('DROP TABLE IF EXISTS tpch.%I CASCADE', _tbl);
    END LOOP;
    RESET client_min_messages;

    EXECUTE $ddl$
create table tpch.nation (
    n_nationkey  integer     not null,
    n_name       char(25)    not null,
    n_regionkey  integer     not null,
    n_comment    varchar(152),
    primary key (n_nationkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.region (
    r_regionkey  integer     not null,
    r_name       char(25)    not null,
    r_comment    varchar(152),
    primary key (r_regionkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.part (
    p_partkey     integer        not null,
    p_name        varchar(55)    not null,
    p_mfgr        char(25)       not null,
    p_brand       char(10)       not null,
    p_type        varchar(25)    not null,
    p_size        integer        not null,
    p_container   char(10)       not null,
    p_retailprice decimal(15,2)  not null,
    p_comment     varchar(23)    not null,
    primary key (p_partkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.supplier (
    s_suppkey     integer        not null,
    s_name        char(25)       not null,
    s_address     varchar(40)    not null,
    s_nationkey   integer        not null,
    s_phone       char(15)       not null,
    s_acctbal     decimal(15,2)  not null,
    s_comment     varchar(101)   not null,
    primary key (s_suppkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.partsupp (
    ps_partkey     integer        not null,
    ps_suppkey     integer        not null,
    ps_availqty    integer        not null,
    ps_supplycost  decimal(15,2)  not null,
    ps_comment     varchar(199)   not null,
    primary key (ps_partkey, ps_suppkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.customer (
    c_custkey     integer        not null,
    c_name        varchar(25)    not null,
    c_address     varchar(40)    not null,
    c_nationkey   integer        not null,
    c_phone       char(15)       not null,
    c_acctbal     decimal(15,2)  not null,
    c_mktsegment  char(10)       not null,
    c_comment     varchar(117)   not null,
    primary key (c_custkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.orders (
    o_orderkey       integer        not null,
    o_custkey        integer        not null,
    o_orderstatus    char(1)        not null,
    o_totalprice     decimal(15,2)  not null,
    o_orderdate      date           not null,
    o_orderpriority  char(15)       not null,
    o_clerk          char(15)       not null,
    o_shippriority   integer        not null,
    o_comment        varchar(79)    not null,
    primary key (o_orderkey)
);
    $ddl$;

    EXECUTE $ddl$
create table tpch.lineitem (
    l_orderkey       integer        not null,
    l_partkey        integer        not null,
    l_suppkey        integer        not null,
    l_linenumber     integer        not null,
    l_quantity       decimal(15,2)  not null,
    l_extendedprice  decimal(15,2)  not null,
    l_discount       decimal(15,2)  not null,
    l_tax            decimal(15,2)  not null,
    l_returnflag     char(1)        not null,
    l_linestatus     char(1)        not null,
    l_shipdate       date           not null,
    l_commitdate     date           not null,
    l_receiptdate    date           not null,
    l_shipinstruct   char(25)       not null,
    l_shipmode       char(10)       not null,
    l_comment        varchar(44)    not null,
    primary key (l_orderkey, l_linenumber)
);
    $ddl$;

    RETURN 'Created 8 TPC-H tables in tpch schema';
END;
$func$;

-- =============================================================================
-- gen_data(scale, parallel) — generate and load data via dbgen binary
-- parallel defaults to 1 (sequential). Set parallel > 1 to run that many
-- dbgen workers simultaneously, which can dramatically cut wall-clock time.
-- Example: SELECT tpch.gen_data(10, 8);  -- SF=10 with 8 parallel workers
--
-- Note: unlike dsdgen, dbgen writes all chunks to the same filenames, so each
-- worker gets its own subdirectory (<data_dir>/chunk_N/) to avoid conflicts.
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.gen_data(scale_factor INTEGER, parallel INTEGER DEFAULT 1)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpch_dir TEXT;
    _data_dir TEXT;
    _tbl TEXT;
    _tables TEXT[] := ARRAY[
        'nation','region','part','supplier',
        'partsupp','customer','orders','lineitem'
    ];
    _start_ts TIMESTAMPTZ;
    _row_count BIGINT;
    _total_rows BIGINT := 0;
    _gen_cmd TEXT;
    _load_cmd TEXT;
BEGIN
    IF parallel < 1 THEN
        RAISE EXCEPTION 'parallel must be >= 1';
    END IF;

    _tpch_dir := tpch._resolve_dir('tpch_dir', 'tpch_dbgen');

    SELECT value INTO _data_dir FROM tpch.config WHERE key = 'data_dir';
    IF _data_dir IS NULL OR _data_dir = '' THEN
        _data_dir := '/tmp/tpch_data';
    END IF;

    -- Truncate all tables
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('TRUNCATE tpch.%I CASCADE', _tbl);
    END LOOP;

    -- Generate data using dbgen binary.
    -- With parallel > 1: each worker gets its own subdirectory because dbgen
    -- always writes to the same filenames (no chunk suffix), so concurrent
    -- workers would corrupt each other if they shared a directory.
    _start_ts := clock_timestamp();
    IF parallel = 1 THEN
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _data_dir);
        _gen_cmd := format(
            'cd %s/dbgen && DSS_PATH=%s DSS_CONFIG=%s/dbgen ./dbgen -s %s -f',
            _tpch_dir, _data_dir, _tpch_dir, scale_factor);
    ELSE
        -- Create per-worker subdirs and launch all workers in parallel via xargs.
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('seq 1 %s | xargs -P %s -I{} mkdir -p %s/chunk_{}',
                   parallel, parallel, _data_dir));
        _gen_cmd := format(
            'cd %s/dbgen && seq 1 %s | xargs -P %s -I{} sh -c ''DSS_PATH=%s/chunk_{} DSS_CONFIG=%s/dbgen ./dbgen -s %s -f -C %s -S {}''',
            _tpch_dir, parallel, parallel, _data_dir, _tpch_dir, scale_factor, parallel);
    END IF;
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _gen_cmd);
    RAISE NOTICE 'Data generation completed in % seconds',
        extract(epoch from clock_timestamp() - _start_ts);

    -- Load each table via COPY FROM PROGRAM (strip trailing pipe).
    -- With parallel > 1, cat the same-named file from every chunk subdirectory.
    FOREACH _tbl IN ARRAY _tables LOOP
        _start_ts := clock_timestamp();
        BEGIN
            IF parallel = 1 THEN
                _load_cmd := format('sed ''s/|$//'' %s/%s.tbl', _data_dir, _tbl);
            ELSE
                _load_cmd := format('cat %s/chunk_*/%s.tbl | sed ''s/|$//''',
                                    _data_dir, _tbl);
            END IF;
            EXECUTE format(
                'COPY tpch.%I FROM PROGRAM %L WITH (DELIMITER %L, NULL %L)',
                _tbl, _load_cmd, '|', ''
            );
            GET DIAGNOSTICS _row_count = ROW_COUNT;
            _total_rows := _total_rows + _row_count;
            RAISE NOTICE 'Loaded % rows into % (% sec)',
                _row_count, _tbl,
                round(extract(epoch from clock_timestamp() - _start_ts)::numeric, 2);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to load %: %', _tbl, SQLERRM;
        END;
    END LOOP;

    -- ANALYZE all tables
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('ANALYZE tpch.%I', _tbl);
    END LOOP;

    -- Clean up .tbl files to free disk space
    IF parallel = 1 THEN
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('rm -f %s/*.tbl', _data_dir));
    ELSE
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
            format('rm -rf %s/chunk_*', _data_dir));
    END IF;
    RAISE NOTICE 'Cleaned up .tbl files from %', _data_dir;

    -- Save scale factor so gen_query() can pick it up
    UPDATE tpch.config SET value = scale_factor::TEXT WHERE key = 'scale_factor';
    IF NOT FOUND THEN
        INSERT INTO tpch.config (key, value) VALUES ('scale_factor', scale_factor::TEXT);
    END IF;

    RETURN format('Loaded %s total rows at SF=%s (parallel=%s)', _total_rows, scale_factor, parallel);
END;
$func$;

-- =============================================================================
-- gen_query(seed) — generate 22 queries via qgen, fix, store
--   Scale is read from gen_data() automatically.
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.gen_query(seed INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpch_dir TEXT;
    _query_dir TEXT;
    _scale INTEGER;
    _cmd TEXT;
    _raw TEXT;
    _fixed TEXT;
    _i INTEGER;
    _seed_arg TEXT := '';
    _count INTEGER := 0;
BEGIN
    _tpch_dir := tpch._resolve_dir('tpch_dir', 'tpch_dbgen');
    _query_dir := tpch._resolve_dir('query_dir', 'tpch_query');

    -- Read scale factor saved by gen_data(), default to 1
    SELECT value::INTEGER INTO _scale FROM tpch.config WHERE key = 'scale_factor';
    IF _scale IS NULL THEN
        _scale := 1;
    END IF;

    IF seed IS NOT NULL THEN
        _seed_arg := format(' -r %s', seed);
    END IF;

    DELETE FROM tpch.query;

    -- Create queries output directory
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _query_dir);

    SET LOCAL client_min_messages = warning;
    DROP TABLE IF EXISTS _qgen_out;
    RESET client_min_messages;
    CREATE TEMP TABLE _qgen_out (line TEXT) ON COMMIT DROP;

    FOR _i IN 1..22 LOOP
        TRUNCATE _qgen_out;

        _cmd := format('cd %s/dbgen && DSS_QUERY=%s/dbgen/queries ./qgen -s %s -d %s%s | tr -d ''\r''',
            _tpch_dir, _tpch_dir, _scale, _i, _seed_arg);

        BEGIN
            EXECUTE format('COPY _qgen_out FROM PROGRAM %L WITH (DELIMITER E''\x01'')', _cmd);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'qgen failed for query %: %', _i, SQLERRM;
            CONTINUE;
        END;

        SELECT string_agg(line, E'\n') INTO _raw FROM _qgen_out;

        IF _raw IS NULL OR btrim(_raw) = '' THEN
            RAISE WARNING 'qgen produced no output for query %', _i;
            CONTINUE;
        END IF;

        _fixed := tpch._fix_query(_i, _raw);
        INSERT INTO tpch.query (query_id, query_text) VALUES (_i, _fixed);
        _count := _count + 1;

        -- Write query to file
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _fixed,
                format('cat > %s/query%s.sql', _query_dir, _i));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write query%s.sql: %', _i, SQLERRM;
        END;
    END LOOP;

    SET LOCAL client_min_messages = warning;
    DROP TABLE IF EXISTS _qgen_out;
    RESET client_min_messages;

    RETURN format('Generated and stored %s queries (scale=%s)', _count, _scale);
END;
$func$;

-- =============================================================================
-- show(qid) — return query text
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.show(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpch.query WHERE query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-22)', qid;
    END IF;
    RETURN _sql;
END;
$func$;

-- =============================================================================
-- exec(qid) — execute a single query, record results
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.exec(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC := 0;
    _total_rows BIGINT := 0;
    _status TEXT := 'OK';
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpch.query WHERE query.query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-22)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpch, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;

        BEGIN
            _start_ts := clock_timestamp();
            EXECUTE _stmt;
            GET DIAGNOSTICS _rows = ROW_COUNT;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
            _total_rows := _total_rows + _rows;
        EXCEPTION WHEN OTHERS THEN
            _status := 'ERROR: ' || SQLERRM;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
        END;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);

    INSERT INTO tpch.bench_results (query_id, status, duration_ms, rows_returned)
    VALUES (qid, _status, round(_total_dur, 2), _total_rows);

    RETURN format('query %s: %s, %s ms, %s rows', qid, _status, round(_total_dur, 2), _total_rows);
END;
$func$;

-- =============================================================================
-- bench(mode) — run or explain all 22 queries, save output to results_dir
--   bench()                     — execute all 22, save queryXX.out
--   bench('EXPLAIN')            — explain all 22, save queryXX_explain.out
--   bench('EXPLAIN (COSTS OFF)')— explain with options, save queryXX_explain.out
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.bench(mode TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _qid INTEGER;
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC;
    _total_rows BIGINT;
    _status TEXT;
    _explain_sql TEXT;
    _line TEXT;
    _all_lines TEXT;
    _part INTEGER;
    _num_stmts INTEGER;
    _results_dir TEXT;
    _bench_start TIMESTAMPTZ;
    _is_explain BOOLEAN := false;
    _explain_opts TEXT := '';
    _filename TEXT;
    _ok_count INTEGER := 0;
    _err_count INTEGER := 0;
    _skip_count INTEGER := 0;
    _bench_dur NUMERIC;
    _saved_path TEXT;
BEGIN
    _bench_start := now();
    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpch, public', false);

    IF mode IS NOT NULL AND upper(btrim(mode)) LIKE 'EXPLAIN%' THEN
        _is_explain := true;
        _explain_opts := btrim(regexp_replace(btrim(mode), '^\s*EXPLAIN\s*', '', 'i'));
        IF _explain_opts LIKE '(%' THEN
            _explain_opts := btrim(_explain_opts, '()');
        END IF;
    END IF;

    _results_dir := tpch._resolve_dir('results_dir', 'tpch_results');
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _results_dir);

    FOR _qid IN 1..22 LOOP
        SELECT query_text INTO _sql FROM tpch.query WHERE query.query_id = _qid;
        IF _sql IS NULL THEN
            _skip_count := _skip_count + 1;
            RAISE NOTICE 'query %: SKIP (not found)', _qid;
            CONTINUE;
        END IF;

        _sql := btrim(_sql, E' \t\n\r');
        _sql := rtrim(_sql, ';');
        _stmts := string_to_array(_sql, ';');
        _total_dur := 0;
        _total_rows := 0;
        _status := 'OK';
        _all_lines := '';
        _part := 0;

        SELECT count(*) INTO _num_stmts
        FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

        FOREACH _stmt IN ARRAY _stmts LOOP
            _stmt := btrim(_stmt, E' \t\n\r');
            IF _stmt = '' OR _stmt IS NULL THEN
                CONTINUE;
            END IF;
            _part := _part + 1;

            IF _is_explain THEN
                IF _num_stmts > 1 THEN
                    _all_lines := _all_lines
                        || format('-- Statement %s of %s', _part, _num_stmts) || E'\n';
                END IF;
                -- DDL statements (CREATE, DROP, etc.) cannot be EXPLAINed — execute them
                IF upper(ltrim(_stmt)) ~ '^(CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s' THEN
                    BEGIN
                        _start_ts := clock_timestamp();
                        EXECUTE _stmt;
                        _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                        _total_dur := _total_dur + _dur;
                        _all_lines := _all_lines || '-- (executed DDL)' || E'\n';
                    EXCEPTION WHEN OTHERS THEN
                        _status := 'ERROR: ' || SQLERRM;
                        _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
                        _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                        _total_dur := _total_dur + _dur;
                    END;
                ELSE
                    IF _explain_opts <> '' THEN
                        _explain_sql := format('EXPLAIN (%s) %s', _explain_opts, _stmt);
                    ELSE
                        _explain_sql := 'EXPLAIN ' || _stmt;
                    END IF;
                    BEGIN
                        _start_ts := clock_timestamp();
                        FOR _line IN EXECUTE _explain_sql LOOP
                            _all_lines := _all_lines || _line || E'\n';
                        END LOOP;
                        _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                        _total_dur := _total_dur + _dur;
                    EXCEPTION WHEN OTHERS THEN
                        _status := 'ERROR: ' || SQLERRM;
                        _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
                        _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                        _total_dur := _total_dur + _dur;
                    END;
                END IF;
            ELSE
                BEGIN
                    _start_ts := clock_timestamp();
                    EXECUTE _stmt;
                    GET DIAGNOSTICS _rows = ROW_COUNT;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _total_rows := _total_rows + _rows;
                    _all_lines := _all_lines
                        || format('Statement %s: %s rows, %s ms',
                                  _part, _rows, round(_dur, 2)) || E'\n';
                EXCEPTION WHEN OTHERS THEN
                    _status := 'ERROR: ' || SQLERRM;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
                END;
            END IF;
        END LOOP;

        INSERT INTO tpch.bench_results (query_id, status, duration_ms, rows_returned)
        VALUES (_qid, _status, round(_total_dur, 2), _total_rows);

        IF _is_explain THEN
            _filename := format('query%s_explain.out', _qid);
        ELSE
            _filename := format('query%s.out', _qid);
        END IF;
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _all_lines,
                format('cat > %s/%s', _results_dir, _filename));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write %: %', _filename, SQLERRM;
        END;

        IF _status = 'OK' THEN
            _ok_count := _ok_count + 1;
        ELSE
            _err_count := _err_count + 1;
        END IF;

        RAISE NOTICE 'query %: % (% ms)', _qid, _status, round(_total_dur);
    END LOOP;

    -- Update bench_summary table with latest run
    TRUNCATE tpch.bench_summary;
    INSERT INTO tpch.bench_summary (query_id, status, duration_ms, rows_returned, run_ts)
    SELECT query_id, status, duration_ms, rows_returned, run_ts
    FROM tpch.bench_results
    WHERE run_ts >= _bench_start
    ORDER BY query_id;

    -- Write summary CSV
    BEGIN
        EXECUTE format(
            'COPY (SELECT query_id, status, duration_ms, rows_returned '
            'FROM tpch.bench_summary ORDER BY query_id) '
            'TO PROGRAM %L WITH (FORMAT csv, HEADER)',
            format('cat > %s/summary.csv', _results_dir));
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Could not write summary.csv: %', SQLERRM;
    END;

    _bench_dur := round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);

    PERFORM set_config('search_path', _saved_path, false);

    RETURN format('Completed: %s OK, %s errors, %s skipped in %s sec. Results: %s/summary.csv',
        _ok_count, _err_count, _skip_count, _bench_dur, _results_dir);
END;
$func$;

-- =============================================================================
-- explain(qid, opts) — EXPLAIN a single query, return plan to client
-- =============================================================================
CREATE OR REPLACE FUNCTION tpch.explain(qid INTEGER, opts TEXT DEFAULT '')
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _explain_sql TEXT;
    _line TEXT;
    _part INTEGER := 0;
    _num_stmts INTEGER;
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpch.query WHERE query.query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-22)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpch, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    SELECT count(*) INTO _num_stmts
    FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;
        _part := _part + 1;

        IF _num_stmts > 1 THEN
            RETURN NEXT format('-- Statement %s of %s', _part, _num_stmts);
        END IF;

        -- DDL statements (CREATE, DROP, etc.) cannot be EXPLAINed — execute them
        IF upper(ltrim(_stmt)) ~ '^(CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s' THEN
            EXECUTE _stmt;
            RETURN NEXT '-- (executed DDL)';
        ELSE
            IF opts <> '' THEN
                _explain_sql := format('EXPLAIN (%s) %s', opts, _stmt);
            ELSE
                _explain_sql := 'EXPLAIN ' || _stmt;
            END IF;

            FOR _line IN EXECUTE _explain_sql LOOP
                RETURN NEXT _line;
            END LOOP;
        END IF;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);
END;
$func$;
