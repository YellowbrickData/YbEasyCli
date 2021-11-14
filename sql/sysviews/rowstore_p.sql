/* ****************************************************************************
** rowstore_p()
**
** Details of user tables in the rowstore.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2020.04.26 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**   status | dbs | tables | rows | unflushed_mb | files | file_mb | pct_of_max |
**  --------+-----+--------+------+--------------+-------+---------+------------+
**   NORMAL |   2 |      8 |  240 |            0 |     8 |     256 |        0.0 |
** ...
** ... commit_blks | file_ids | blk_gens |  min_txid  |  max_txid  | flush_rows 
** ...-------------+----------+----------+------------+------------+------------ 
** ...      238984 |       11 |        5 | 4298332481 | 4304077190 |      64221 
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS rowstore_size_detail_t CASCADE
;

CREATE TABLE rowstore_size_detail_t
(
    status         VARCHAR(32)                 
  , dbs            BIGINT               
  , tables         BIGINT               
  , rows           BIGINT               
  , unflushed_mb   DECIMAL (18,2)             
  , files          BIGINT               
  , file_mb        DECIMAL (18,2)              
  , pct_of_max     DECIMAL (18,1)              
  , commit_blks    BIGINT               
  , file_ids       BIGINT               
  , blk_gens       BIGINT               
  , min_txid       BIGINT               
  , max_txid       BIGINT               
  , flush_rows     BIGINT               
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE rowstore_p()
   RETURNS SETOF rowstore_size_detail_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'rowstore_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- SET TRANSACTION       READ ONLY;

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT *
   FROM
   ( SELECT
         yb_yrs_rowstore_status::VARCHAR(32)                             AS status
      FROM sys.yb_yrs_rowstore_status()
   ) s
   CROSS JOIN
   (  SELECT
         COUNT( DISTINCT r.database_id )                                 AS dbs
       , COUNT(*)                                                        AS tables
       , SUM( r.row_count )                                              AS rows
       , ROUND( SUM( r.unflushed_bytes ) / 1024.0^2 )::DECIMAL (18,2)    AS unflushed_mb
       , SUM( r.files_used )                                             AS files
       , ROUND( SUM(files_used * file_size) / 1024.0^2 )::DECIMAL (18,2) AS file_mb       
       , ROUND( (SUM(files_used)/file_limit) * 100.0, 1)::DECIMAL (18,1) AS pct_of_max     
      FROM yb_yrs_tables()  r
         JOIN sys.database  d ON r.database_id = d.database_id
      GROUP BY r.file_limit, r.file_size
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
         COUNT(*)                                                        AS flush_rows
      FROM sys.yrs_flush
   ) f
   ';

   RETURN QUERY EXECUTE _sql; 

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;

-- ALTER FUNCTION rowstore_p()
--   SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION rowstore_p() IS 
'Description:
Rowstore overal metrics including size of data in user tables.
  
Examples:
  SELECT * FROM rowstore_p() 
  SELECT * FROM rowstore_p() ORDER BY total_size DESC LIMIT 30;
  
Arguments:
. none

Notes:
. This is only the space consumed by Yellowbrick rowstore (e.g. in the front-end).
. The columnstore and catalog storage space is not included.

Version:
. 2020.04.26 - Yellowbrick Technical Support 
'
;