/* yrs_file_type_smry.sql
** 
** Summary of yrs space usage by yrs file type.
** 
** Example Result:
** 
**          type         | files | gb | dbs | tbls |   mn_txid   |   mx_txid   |  crnt_txid  | dlta_mx  | dlta_crnt
** ----------------------+-------+----+-----+------+-------------+-------------+-------------+----------+-----------
**  unflushed_data_files |     2 |  1 |   1 |    2 | 90221271225 | 90222751452 | 90222751969 |  1480227 |   1480744
**  flushed_data_files   |     2 |  1 |   1 |    2 | 90221271225 | 90222751452 | 90222751969 |  1480227 |   1480744
**  commit_files         |     1 |  1 |     |      | 90194607341 | 90221489959 | 90222751969 | 26882618 |  28144628
**  
** yrs functions: 
** . sys.yb_yrs_rowstore_status() - Returns state of yrs. Should be NORMAL.
** . sys.yrs_flush                - Record of committed flush operations per table. Drop also counts as a flushes.
**                                   The difference is that the rows don't make it to the backend.
** . yb_yrs_data_files            - 
** . yb_yrs_commit_files()        - Committed files waiting to be freed.
**                                  A large size implies a problem; i.e a backup event horizon 
**                                  and/or tables continually written to so housekeeping can never clear them out.
** . yb_yrs_tables()              - Per table info for tables that have data in the rowstore 
** . yb_yrs_delete_files          - YRS files whose rows have been flushed but files not deleted.
**                                  The files should be deleted on next YB restart or yb_yrs_delete_unused_files();
** 
** 
** Revision History
** . 2026.02.10 (rek) - Added types CTE and LEFT JOIN
** . 2024.11.25 (rek) - Initial version
*/

\o delete.me
SET work_mem TO 1000000;
\o

WITH yrs_file_types AS 
( SELECT           'unflushed_data_files' AS type
  UNION ALL SELECT 'flushed_data_files'   AS type
  UNION ALL SELECT 'commit_files'         AS type
  UNION ALL SELECT 'delete_files'         AS type
)
, unflushed_data_files AS
   (  SELECT
         0                           AS sort_key
       , 'unflushed_data_files'      AS type
       , COUNT(*)                    AS files
       , ceil((files * 30) / 1024.0) AS gb
       , COUNT(DISTINCT database_id) AS databases
       , COUNT(DISTINCT table_id )   AS tables
       , MIN( lowest_txid )          AS min_txid
       , MAX( highest_txid )         AS max_txid
      FROM yb_yrs_data_files()       AS yrsdf
      GROUP BY 1, 2
   )
, flushed_data_files AS
   (  SELECT
         1                           AS sort_key
       , 'flushed_data_files'        AS type
       , COUNT(*)                    AS files
       , ceil((files * 30) / 1024.0) AS gb
       , COUNT(DISTINCT database_id) AS databases
       , COUNT(DISTINCT table_id )   AS tables
       , MIN( lowest_txid )          AS min_txid
       , MAX( highest_txid )         AS max_txid
      FROM yb_yrs_data_files()       AS yrsdf
      GROUP BY 1, 2
   )   
 , commit_files AS
   (  SELECT
         2                             AS sort_key
       , 'commit_files'                AS type
       , COUNT(DISTINCT file_id)       AS files
       , ceil((files * 30 ) / 1024.0 ) AS gb
       , NULL                          AS databases
       , NULL                          AS tables
       , MIN(block_highest_txid)       AS min_txid
       , MAX(block_highest_txid)       AS max_txid
      FROM yb_yrs_commit_files()       AS yrscf
      GROUP BY 1, 2
   )
 , delete_files AS
   (  SELECT
         3                                  AS sort_key
       , 'delete_files'                     AS type
       , COUNT(DISTINCT yrsdf.data_file_id) AS files
       , ceil((files * 30) / 1024.0)        AS gb
       , COUNT(DISTINCT st.database_id)     AS databases
       , COUNT(DISTINCT yrsdf.table_id)     AS tables
       , MIN(yrsdf.txid)                    AS min_txid
       , MAX(yrsdf.txid)                    AS max_txid
      FROM yb_yrs_delete_files()            AS yrsdf
      LEFT JOIN sys.table                   AS st ON yrsdf.table_id = st.table_id
      GROUP BY 1, 2
   )
, files_union AS 
(   SELECT * FROM unflushed_data_files
    UNION ALL
    SELECT * FROM flushed_data_files
    UNION ALL
    SELECT * FROM commit_files
    UNION ALL
    SELECT * FROM delete_files
)
, all_files as
( SELECT * FROM yrs_file_types LEFT JOIN files_union USING (type)
)

SELECT
   type                   AS type
 , files                  AS files
 , gb                     AS gb
 , databases              AS dbs
 , tables                 AS tbls
 , min_txid               AS mn_txid
 , max_txid               AS mx_txid
 , txid_current()         AS crnt_txid
 , (max_txid  - min_txid) AS dlta_mx
 , (crnt_txid - min_txid) AS dlta_crnt
FROM all_files
ORDER BY sort_key
;