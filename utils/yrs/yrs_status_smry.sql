/* user_rowstore_status_smry.sql
** 
** Summary of flushed and unflushed tables in the yrs (yb user rowstore)
** 
** Example result:
**    status | dbs | usr_tbls | sys_tbls | ttl_tbls | rows  | unflushed_mb | files | file_mb | commit_blks | file_ids | blk_gens |  min_txid   |  max_txid   | flush_recs
**   --------+-----+----------+----------+----------+-------+--------------+-------+---------+-------------+----------+----------+-------------+-------------+------------
**    NORMAL |   3 |       11 |        0 |       11 | 10081 |         2.00 |    10 |  320.00 |       17837 |        1 |        9 | 90194404108 | 90228797213 |         62
**  
** yrs functions: 
** . sys.yb_yrs_rowstore_status() - Returns state of yrs. Should be NORMAL.
** . sys.yrs_flush                - Record of committed flush operations per table. 
*                                   Drop also counts as a flush operation. Only difference is that the rows don't make it to the backend.
** . yb_yrs_commit_files()        - A large file implies a problem with a backup event horizon and/or tables that are continually being written to so housekeeping can never clear the out.
** . yb_yrs_tables()              - Per table info for tables that have data in the rowstore 
** 
** Revision History:
** . 2026.02.04 (rek) - Replaced JOIN to sys.database with pg_database as sys tables report db as 4418 (_YBPG_stats)
** . 2026.02.02 (rek) - Remove group by file limit and size and add user & sys table columns
** . 2024.09.03 (rek) - Initial version
*/

SELECT *
   FROM
   ( SELECT
         yb_yrs_rowstore_status::VARCHAR(32)                             AS status
      FROM sys.yb_yrs_rowstore_status()
   ) s
   CROSS JOIN
   (  SELECT
         COUNT( DISTINCT r.database_id )                                 AS dbs
       , SUM(IIF(table_id > 16383, 1, 0))                                AS usr_tbls
       , SUM(IIF(table_id < 16384, 1, 0))                                AS sys_tbls
       , COUNT(*)                                                        AS ttl_tbls
       , SUM( r.row_count )                                              AS rows
       , ROUND( SUM( r.unflushed_bytes ) / 1024.0^2 )::DECIMAL (18,2)    AS unflushed_mb
       , SUM( r.files_used )                                             AS files
       , ROUND( SUM(files_used * file_size) / 1024.0^2 )::DECIMAL (18,2) AS file_mb          
      FROM yb_yrs_tables()  r
         JOIN pg_database d ON r.database_id = d.oid
   ) r
   CROSS JOIN
   (  SELECT
         COUNT(*)                                                        AS commit_blks
       , COUNT( DISTINCT file_id )                                       AS file_ids
       , COUNT( DISTINCT block_generation )                              AS blk_gens
       , MIN( record_txid )                                              AS min_txid
       , MAX( block_highest_txid )                                       AS max_txid
      FROM yb_yrs_commit_files()
   ) c
   CROSS JOIN
   (  SELECT
         COUNT(*)                                                        AS flush_recs
      FROM sys.yrs_flush
   ) f
;
