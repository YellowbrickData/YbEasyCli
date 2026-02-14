/* yrs_tables.sql
**
** Yellowbrick User RowStore (YRS) table metrics for tables having unflushed rows.
**
** Example Results
**    db_name | schema_name | table_name | rows | unflushed_mb | used_mb | files | file_mb
**   ---------+-------------+------------+------+--------------+---------+-------+---------
**    kick    | public      | i          |   15 |         0.00 |    0.00 |     1 |      32
**    kick    | public      | i2         |    6 |         0.00 |    0.00 |     1 |      32
**
** Revision History:
** . 2026.02.04 (rek) - fix for system tables not being shown
** . 2026.01.26 (rek)
*/

-- User Tables
SELECT
   NVL(d.name,'missing')::VARCHAR(128)                            AS db_name
 , NVL(s.name,'missing')::VARCHAR(128)                            AS schema_name
 , NVL(t.name,'missing')::VARCHAR(128)                            AS table_name
 , r.table_id                                                     AS table_id
 , SUM(row_count)                                                 AS rows
 , ROUND( SUM( r.unflushed_bytes ) / 1024.0^2,2 )::NUMERIC(12,2)  AS unflushed_mb
 , ROUND( SUM( r.used_bytes )      / 1024.0^2,2 )::NUMERIC(12,2)  AS used_mb
 , SUM( r.files_used )                                            AS files
 , ROUND( SUM(files_used * file_size) / 1024.0^2 )::NUMERIC(12,0) AS file_mb       
FROM yb_yrs_tables()     AS r
  LEFT JOIN sys.table    AS t  ON r.table_id    = t.table_id
  LEFT JOIN sys.schema   AS s  ON t.database_id = s.database_id AND t.schema_id = s.schema_id
  LEFT JOIN sys.database AS d  ON t.database_id = d.database_id
WHERE r.table_id >= 16384
GROUP BY db_name, schema_name, table_name, r.table_id  

-- System tables. Treat them all as being in the database "yellowbrick"
UNION ALL
SELECT
   NVL(d.name,'missing')::VARCHAR(128)                            AS db_name
 , NVL(s.nspname,'missing')::VARCHAR(128)                         AS schema_name
 , NVL(t.relname,'missing')::VARCHAR(128)                         AS table_name
 , r.table_id                                                     AS table_id
 , SUM(row_count)                                                 AS rows
 , ROUND( SUM( r.unflushed_bytes ) / 1024.0^2,2 )::NUMERIC(12,2)  AS unflushed_mb
 , ROUND( SUM( r.used_bytes )      / 1024.0^2,2 )::NUMERIC(12,2)  AS used_mb
 , SUM( r.files_used )                                            AS files
 , ROUND( SUM(files_used * file_size) / 1024.0^2 )::NUMERIC(12,0) AS file_mb       
FROM yb_yrs_tables()     AS r
  LEFT JOIN pg_class     AS t  ON r.table_id     = t.oid
  LEFT JOIN pg_namespace AS s  ON t.relnamespace = s.oid
  LEFT JOIN sys.database AS d  ON 4400           = d.database_id
WHERE r.table_id < 16384
GROUP BY db_name, schema_name, table_name, r.table_id  

ORDER BY db_name, schema_name, table_name, table_id
;
