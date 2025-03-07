-- gen_replication_stop_start.sql
--
-- Generate SQL scripts stop and to restart all replication jobs:
-- . do_replication_stop.out.sql
-- . do_replication_restart.out.sql
-- 
-- 2024-05-23

\a
\t ON

\o do_replication_stop.out.sql
SELECT 
   'ALTER DATABASE ' || QUOTE_IDENT(d.name) || ' ALTER REPLICA ' || QUOTE_IDENT(r.name) || ' PAUSE;'
FROM sys.replica r
INNER JOIN sys.database d ON r.database_id = d.database_id
WHERE status = 'RUNNING' 
ORDER BY 1
; 
\o

-- The restart includes a 60 second sleep between the restart of each replica.		
\o do_replication_restart.out.sql
SELECT
 'SELECT ' || QUOTE_LITERAL(r.name) || ' AS starting_replica_with_wait ;' || CHR(10)
 ||'ALTER DATABASE ' || QUOTE_IDENT(d.name) || ' ALTER REPLICA ' || QUOTE_IDENT(r.name) || ' RESUME;' || CHR(10)
 ||'SELECT sys.inject_idle(60000) AS waited_60_secs FROM sys.const;' || CHR(10)
FROM sys.replica r
INNER JOIN sys.database d ON r.database_id = d.database_id
WHERE status = 'RUNNING'
ORDER BY 1
;     
\o

\a 
\t OFF
\echo Generated do_replication_stop.out.sql and do_replication_restart.out.sql scripts.
\echo 
