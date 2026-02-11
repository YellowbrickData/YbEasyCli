/* gen_yrs_flush_ys_table.sql
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
**
** Example Results:
**   \c yellowbrick;
**   \echo Thread 1: ................................Flushing: sys._log_query
**   yflush sys._log_query;
**
** Revision History:
** . 2026.02.05 (rek) - Initial version (derived from gen_yrs_flush_user_tables.sql)
** 
*/

-- generate 4 files so each can be run in a separate thread
\set num_groups 4

SELECT
   '\c yellowbrick;' || CHR(10)
   || '\echo Thread ' || :grouping || ': ' || REPEAT('.',32) || 'Flushing: ' || quote_ident(s.nspname) || '.' || quote_ident(t.relname) || CHR(10)
   || 'yflush ' || quote_ident(s.nspname) || '.' || quote_ident(t.relname) || ';' || CHR(10)
FROM yb_yrs_tables()     AS y
  LEFT JOIN pg_class     AS t ON y.table_id     = t.oid
  LEFT JOIN pg_namespace AS s ON t.relnamespace = s.oid
WHERE y.unflushed_bytes > 0 
  AND (y.table_id::INT8 %:num_groups) = (:grouping - 1)
  AND y.table_id < 16384
GROUP BY s.nspname, t.relname
ORDER BY s.nspname, t.relname
;
