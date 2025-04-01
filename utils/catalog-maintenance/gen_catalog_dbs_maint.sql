-- gen_catalog_dbs_maint.sql
--
-- Generates 4 SQL files (catalog_dbs_maint_[1-4].sql), each of a different 1/4
-- of the user databases that run catalog_db_maint.sql. 
-- These 4 SQL files are each run in a separate thread (fork) by catalog_dbs_maint.sh
--
-- Alignment is disabled and tuples only enabled which is necessary for the SQL
-- to be correctly generated.
--
-- 2024-10-22 - Add DONE message.
-- 2024-04-30

\a
\t ON

\echo Generating catalog_dbs_maint_1.out.sql
\o catalog_dbs_maint_1.out.sql
\qecho '\\set prc 1'
SELECT '\c ' || quote_ident(name) || CHR(10)
  || '\i catalog_db_maint.sql' || CHR(10)
FROM sys.database 
WHERE database_id %4 = 0
ORDER BY name
;

\echo Generating catalog_dbs_maint_2.out.sql
\o catalog_dbs_maint_2.out.sql
\qecho '\\set prc 2'
SELECT '\c ' || quote_ident(name) || CHR(10)
  || '\i catalog_db_maint.sql' || CHR(10)
FROM sys.database 
WHERE database_id %4 = 1
ORDER BY name
;

\echo Generating catalog_dbs_maint_3.out.sql
\o catalog_dbs_maint_3.out.sql
\qecho '\\set prc 3'
SELECT '\c ' || quote_ident(name) || CHR(10)
  || '\i catalog_db_maint.sql' || CHR(10)
FROM sys.database 
WHERE database_id %4 = 2
ORDER BY name
;

\echo Generating catalog_dbs_maint_4.out.sql
\o catalog_dbs_maint_4.out.sql
\qecho '\\set prc 4'
SELECT '\c ' || quote_ident(name) || CHR(10)
  || '\i catalog_db_maint.sql' || CHR(10)
FROM sys.database 
WHERE database_id %4 = 3
ORDER BY name
;

\a
\t OFF

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo DONE. RUN catalog_dbs_maint.sh to maintenance all user databases.
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
