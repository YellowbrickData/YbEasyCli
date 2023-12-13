/* ****************************************************************************
** rowstore_by_table_p()
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
** . 2023.12.12 - Fix for schema join bug.
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | schema_name |     table_name     | unflushed_mb | used_mb | files | file_mb
** -------------+-------------+--------------------+--------------+---------+-------+---------
**  azi         | public      | test1              |            0 |       0 |    30 |     960
**  azi         | public      | test2              |            0 |       0 |    30 |     960
**  mfg_g2      | raw         | dcs_blade          |            0 |       0 |     1 |      32
**  mfg_g2      | raw         | dcs_drives         |            0 |       0 |     1 |      32
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS rowstore_by_table_t CASCADE
;

CREATE TABLE rowstore_by_table_t
(
   db_name        VARCHAR(128)                                
 , schema_name    VARCHAR(128)              
 , table_name     VARCHAR(128)   
 , rows           BIGINT 
 , unflushed_mb   NUMERIC(12,2)                             
 , used_mb        NUMERIC(12,2)                             
 , files          BIGINT                              
 , file_mb        NUMERIC(12,0)                             
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE rowstore_by_table_p()
   RETURNS SETOF rowstore_by_table_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'rowstore_by_table_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';

   _sql := 'SELECT
      NVL(d.name,''yellowbrick'')::VARCHAR(128)                      AS db_name
    , s.name::VARCHAR(128)                                           AS schema_name
    , t.name::VARCHAR(128)                                           AS table_name
    , SUM(row_count)                                                 AS rows
    , ROUND( SUM( r.unflushed_bytes ) / 1024.0^2,2 )::NUMERIC(12,2)  AS unflushed_mb
    , ROUND( SUM( r.used_bytes )      / 1024.0^2,2 )::NUMERIC(12,2)  AS used_mb
    , SUM( r.files_used )                                            AS files
    , ROUND( SUM(files_used * file_size) / 1024.0^2 )::NUMERIC(12,0) AS file_mb       
   FROM yb_yrs_tables()      AS r
      LEFT JOIN sys.table    AS t  ON r.table_id    = t.table_id
      JOIN sys.schema        AS s  ON t.database_id = s.database_id AND t.schema_id = s.schema_id
      LEFT JOIN sys.database AS d  ON t.database_id = d.database_id
   GROUP BY r.table_id, d.name, s.name, t.name, r.file_limit, r.file_size   
   ORDER BY db_name, schema_name, table_name
   ';

   RETURN QUERY EXECUTE _sql; 

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;   
$proc$ 
;


COMMENT ON FUNCTION rowstore_by_table_p() IS 
$cmnt$Description:
Size of rowstore data in user tables across all databases.
  
Examples:
  SELECT * FROM rowstore_by_table_p();
  SELECT * FROM rowstore_by_table_p() ORDER BY used_mb DESC LIMIT 30;
  
Arguments:
. none

Notes:
. This is only the space consumed by rows still in the rowstore (e.g. in the front-end).
. The columnstore storage space is not included in these numbers.

Version:
. 2023.12.12 - Yellowbrick Technical Support
$cmnt$
;