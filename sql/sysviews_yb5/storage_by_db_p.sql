/* ****************************************************************************
** storage_by_db_p()
**
** Appliance storage committed data storage space used by database.
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
** Version History:
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.08 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support
** . 2020.02.16 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | rows_mil | cmpr_gb | uncmpr_gb | appl_gb | pct
** -------------+----------+---------+-----------+---------+------
**  temp space  |        0 |   24418 |     24418 |  122091 | 20.0
**  premdb      |        0 |       3 |        20 |  122091 |  0.0
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS storage_by_db_t CASCADE
;

CREATE TABLE storage_by_db_t
   (
      db_name   VARCHAR( 128 )
    , tables    INTEGER
    , rows_mil  NUMERIC( 12, 0 )
    , cmpr_gb   NUMERIC( 12, 0 )
    , uncmpr_gb NUMERIC( 12, 0 )
    , appl_pct  NUMERIC( 12, 1 )
    , appl_gb   NUMERIC( 12, 0 )    
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE storage_by_db_p(_yb_util_filter VARCHAR DEFAULT 'TRUE')
   RETURNS SETOF storage_by_db_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT := '';

   _fn_name   VARCHAR(256) := 'storage_by_db_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN  

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   _sql := 'WITH appliance_storage AS
         ( SELECT
            ROUND( SUM( scratch_bytes / 1024.0^3 ), 3 ) ::numeric( 15, 3 ) AS spill_gb
          , ROUND( SUM( total_bytes   / 1024.0^3 ), 3 ) ::numeric( 15, 3 ) AS total_gb
         FROM
            sys.storage
         )
      , db AS (
         SELECT
           d.name::VARCHAR( 128 )                                                 AS db_name
         , COUNT( DISTINCT t.table_id )::INTEGER                                  AS tables
         , ROUND( SUM( ts.rows_columnstore )   / 1000000.0, 0 )::numeric( 12, 0 ) AS rows_mil
         , ROUND( SUM( ts.compressed_bytes )   / 1024.0^3, 0 )::numeric( 12, 0 )  AS cmpr_gb
         , ROUND( SUM( ts.uncompressed_bytes ) / 1024.0^3, 0 )::numeric( 12, 0 )  AS uncmpr_gb
         , ROUND(( cmpr_gb / MAX( aps.total_gb ) ) * 100.0, 1 ) ::numeric( 12, 1 )AS appl_pct
         , ROUND( MAX( aps.total_gb ) )::numeric( 12, 0 )                         AS appl_gb
         FROM
            sys.table_storage            ts
            RIGHT JOIN sys.table         t ON ts.table_id   = t.table_id
            JOIN sys.database            d ON t.database_id = d.database_id
            CROSS JOIN appliance_storage aps
         WHERE
            ts.table_id > 16384
         GROUP BY
            1
         UNION ALL
         SELECT
           ''temp space''::VARCHAR( 128 )                                       AS db_name
         , 0::INTEGER                                                           AS tables
         , 0::numeric( 12, 0 )                                                  AS rows_mil
         , aps.spill_gb::numeric( 12, 0 )                                       AS cmpr_gb
         , aps.spill_gb::numeric( 12, 0 )                                       AS uncmpr_gb
         , ROUND(( aps.spill_gb / aps.total_gb ) * 100.0, 1 )::numeric( 12, 1 ) AS pct       
         , aps.total_gb::numeric( 12, 0 )                                       AS appl_gb
         FROM
            appliance_storage aps
      )
      SELECT * FROM db
      WHERE ' || _yb_util_filter || '
      ORDER BY cmpr_gb DESC
   ';

   --RAISE INFO '_sql=%', _sql;
   RETURN QUERY EXECUTE _sql ;
  
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;     

END;   
$proc$ 
;

   
COMMENT ON FUNCTION storage_by_db_p(VARCHAR) IS 
'Description:
Storage space of committed blocks in user tables aggregated by database.

Examples:
  SELECT * FROM storage_by_db_p();
  
Arguments:
. None  

Version:
. 2021.12.09 - Yellowbrick Technical Support 
'
;