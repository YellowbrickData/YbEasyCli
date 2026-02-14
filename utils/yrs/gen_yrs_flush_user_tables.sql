/* gen_yrs_flush_user_tables.sql
**
** Generate SQL for Yellowbrick system RowStore (YRS) tables having unflushed rows.
**
** Arguments:
** . grouping - which iteration ( 1 to num_groups)
**
** Prerequisites:
** . Must be run as a superuser. Requires access to pg_catalog.pg_* tables.
** . Run from ybsql (or other client that can uses ybsql variables)
**
** Example Results:
**   \c kick;
**   \echo Thread 1: ................................Flushing: public.ic
**   yflush public.ic;
**
** Revision History:
** . 2026.02.05 (rek) - Updated comments and ybsql vars.
** . 2026.01.26 (rek) - Initial version.
*/

-- generate 4 files so each can be run in a separate thread
\set num_groups 4

SELECT
   '\c ' || NVL(d.name,'yellowbrick') || ';' || CHR(10)
   || '\echo Thread ' || :grouping || ': ' || REPEAT('.',32) || 'Flushing: ' || quote_ident(s.name) || '.' || quote_ident(t.name) || CHR(10)
   || 'yflush ' || quote_ident(s.name) || '.' || quote_ident(t.name) || ';' || CHR(10)
FROM yb_yrs_tables()   AS y
  JOIN sys.table       AS t  ON y.table_id    = t.table_id
  JOIN sys.schema      AS s  ON t.database_id = s.database_id AND t.schema_id = s.schema_id
  JOIN sys.database    AS d  ON t.database_id = d.database_id
WHERE y.unflushed_bytes > 0 
  AND (y.table_id::INT8 %:num_groups) = (:grouping - 1)
  AND y.table_id >= 16383 
GROUP BY y.table_id, d.name, s.name, t.name   
ORDER BY d.name, s.name, t.name
;
