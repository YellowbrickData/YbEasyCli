-- db_udf_detail.sql
SELECT 
   current_database() AS database_name
 , n.nspname          AS schema_name
 , p.proname          AS function_name
 , l.lanname          AS language
FROM pg_proc      AS p
JOIN pg_language  AS l ON p.prolang = l.oid
JOIN pg_namespace AS n ON p.pronamespace = n.oid
WHERE l.lanname IN ('c', 'plpgsql', 'sql', 'ybcpp')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
ORDER BY n.nspname, p.proname
;