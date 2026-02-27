SELECT tpch.gen_query();
SELECT count(*) FROM tpch.query;
SELECT query_id, length(query_text) > 0 AS has_text
  FROM tpch.query WHERE query_id IN (1, 11, 22) ORDER BY query_id;
SELECT length(tpch.show(1)) > 0;
