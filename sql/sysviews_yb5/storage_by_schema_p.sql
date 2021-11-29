/* ****************************************************************************
** public.storage_by_schema_p_v4()
**
** Storage summary by schema within the current database for YBD >= 4.0.
** Does not include sys.* or temp tables. 
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
** . 2021.05.08 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support
** . 2020.02.16 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  db_name | schema_name | tables | rows_mil | cmpr_gb | uncmpr_gb |  db_gb  | pct_of_db
** ---------+-------------+--------+----------+---------+-----------+---------+-----------
**  db_11   | public      |     20 |   100.66 |   25.85 |     80.28 | 8082.97 |       0.3
**  db_11   | templates   |      1 |     0.00 |    0.00 |      0.00 | 8082.97 |       0.0
**  db_11   | sonar       |     39 | 37947.95 | 8057.04 |  24807.29 | 8082.97 |      99.7
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS public.storage_by_schema_t CASCADE
;

CREATE TABLE public.storage_by_schema_t
   (
      db_name     VARCHAR( 128 )
    , schema_name VARCHAR( 128 ) 
    , tables      BIGINT        
    , rows_mil    NUMERIC( 18, 1 )
    , cmpr_gb     NUMERIC( 18, 1 )
    , uncmpr_gb   NUMERIC( 18, 1 )
    , pct_of_db   NUMERIC( 12, 1 )    
    , db_gb       NUMERIC( 18, 1 )
    , pct_of_appl NUMERIC( 12, 1 )    
    , appl_gb     NUMERIC( 12, 0 )
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE public.storage_by_schema_p(
     _db_ilike VARCHAR DEFAULT '%'
   , _schema_ilike VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF public.storage_by_schema_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'storage_by_schema_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* Join on db name is actually more effecient than cross join db_name for YBD < 4.0
   */
   _sql := 'WITH appliance_storage AS
         ( SELECT
            ROUND( SUM( scratch_bytes / 1024.0^3 ), 3 ) ::numeric( 15, 3 ) AS spill_gb
          , SUM( total_bytes )                                             AS total_bytes            
          , ROUND( SUM( total_bytes   / 1024.0^3 ), 3 ) ::numeric( 15, 3 ) AS total_gb
         FROM
            sys.storage
         )
    , table_storage AS
      (  SELECT
            t.database_id               AS database_id
          , t.schema_id                 AS schema_id
          , COUNT (DISTINCT t.table_id) AS tables
          , SUM (ts.rows_columnstore)   AS rows
          , SUM (ts.compressed_bytes)   AS cmpr_bytes
          , SUM (ts.uncompressed_bytes) AS uncmpr_bytes
         FROM
            sys.table              t
            JOIN sys.table_storage ts ON t.table_id    = ts.table_id
         WHERE
            ts.table_id > 16384
            AND t.schema_id = 2200 OR t.schema_id >= 16384
         GROUP BY
            1, 2
      )
    , db_storage AS
      (  SELECT
            database_id       AS database_id
          , SUM (cmpr_bytes)  AS cmpr_bytes
         FROM
            table_storage
         GROUP BY 1
      )

   SELECT
      d.name::VARCHAR(128)                                     AS db_name
    , s.name::VARCHAR(128)                                     AS schema_name
    , ts.tables                                                AS tables
    , ROUND (ts.rows         / 1000000.0, 2) ::NUMERIC (18, 1) AS rows_mil
    , ROUND (ts.cmpr_bytes   / 1024.0^3, 2) ::NUMERIC (18, 1)  AS cmpr_gb
    , ROUND (ts.uncmpr_bytes / 1024.0^3, 2) ::NUMERIC (18, 1)  AS uncmpr_gb
    , CASE WHEN ds.cmpr_bytes = 0 THEN 0::NUMERIC (12, 1)
           ELSE ROUND ( (ts.cmpr_bytes / ds.cmpr_bytes) * 100.0, 1)::NUMERIC (12, 1)  
      END                                                      AS pct_of_db
    , ROUND (ds.cmpr_bytes / 1024.0^3, 2     )::NUMERIC(18, 1) AS db_gb
    , ROUND((ds.cmpr_bytes / aps.total_bytes ) * 100.0, 1 )::NUMERIC( 12, 1 ) 
                                                               AS pct_of_appl       
    , aps.total_gb::NUMERIC( 12, 0 )                           AS appl_gb    
   FROM
      table_storage                ts
      JOIN db_storage              ds ON ts.database_id = ds.database_id
      JOIN sys.schema              s  ON ts.database_id = s.database_id AND ts.schema_id = s.schema_id     
      JOIN sys.database            d  ON ts.database_id = d.database_id   
      CROSS JOIN appliance_storage aps      
   WHERE 
        d.name ILIKE ' || quote_literal( _db_ilike ) || ' 
    AND s.name ILIKE ' || quote_literal( _schema_ilike ) || '
    AND ' || _yb_util_filter || '
   ORDER BY 1, 2, 3
   ';

   --RAISE INFO '_sql=%', _sql;
   RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
   
END;   
$proc$ 
;

-- ALTER FUNCTION storage_by_schema_p( VARCHAR, VARCHAR)
--   SET search_path = pg_catalog,pg_temp;
   
COMMENT ON FUNCTION storage_by_schema_p( VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
Storage summary by schema across one or more databases. 
Does not include sys.* or temp tables. 

Examples:
  SELECT * FROM storage_by_schema_p( );
  SELECT * FROM storage_by_schema_p( ''y_db_name'' );
  SELECT * FROM storage_by_schema_p( ''y_db_name'', ''p%'' );  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the schema name. i.e. ''%fin%''.
                  The default is ''%''
. _schema_ilike - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                  The default is ''%''

Version:
. 2021.05.08 - Yellowbrick Technical Support 
'
;