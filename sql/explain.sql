SET search_path = tpch, public;
CREATE TEMP TABLE explain_results (query_id INT, status TEXT);
DO $$
DECLARE _qid INTEGER;
BEGIN
  FOR _qid IN 1..22 LOOP
    BEGIN
      PERFORM tpch.explain(_qid, 'COSTS OFF');
      INSERT INTO explain_results VALUES (_qid, 'OK');
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO explain_results VALUES (_qid, 'ERROR: ' || SQLERRM);
    END;
  END LOOP;
END;
$$;
SELECT * FROM explain_results ORDER BY query_id;
