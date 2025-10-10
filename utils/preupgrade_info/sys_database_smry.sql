-- sys_database_smry.sql
-- Aggregate of sys.database metrics
-- 2025.03.26

SELECT
   COUNT(*)                                     AS dbs
 , SUM(iif(encoding = 'LATIN9', 1, 0))          AS latin9_dbs
 , SUM(iif(encoding = 'UTF8', 1, 0))            AS utf8_dbs
 , SUM(table_count)                             AS tables
 , SUM(iif(is_readonly = TRUE, 1, 0))           AS readonly_dbs
 , SUM(iif(is_hot_standby = TRUE, 1, 0))        AS hot_standby_dbs
 , SUM(rows_columnstore + rows_rowstore)        AS rows
 , SUM(rows_columnstore)                        AS rows_colstore
 , SUM(rows_rowstore)                           AS rows_rowstore
 , ceil(SUM(compressed_bytes)      / 1024.0^3)  AS compressed_gb
 , ceil(SUM(snapshot_backup_bytes) / 1024.0^3)  AS snapshot_gb
 , ceil(SUM(delete_info_bytes)     / 1024.0^3)  AS del_info_gb
 , ceil(SUM(reclaimable_bytes)     / 1024.0^3)  AS reclaimable_gb
 , ceil(SUM(uncompressed_bytes)    / 1024.0^3)  AS uncompressed_gb
FROM sys.database
;