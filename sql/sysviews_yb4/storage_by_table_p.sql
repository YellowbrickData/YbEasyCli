/* ****************************************************************************
** storage_by_table_p_v4()
**
** Storage summary by schema and table within the current database for YBD >= 4.0
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
** . 2020.10.11 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.04.25 - Yellowbrick Technical Support
** . 2021.11.21 - Integrated with YbEasyCli 
*/

/* ****************************************************************************
**  Example result:
**  db_name | schema_name | table_name  | table_id | rows_mil | cmpr_gb | uncmpr_gb |  db_gb  | pct_of_db
** ---------+-------------+-------------+----------+----------+---------+-----------+---------+-----------
**  dbo     | admin       | ss_hist_t   |    17385 |     0.00 |    0.00 |      0.00 | 7907.76 |      0.00
**  dbo     | public      | search_str1 |    17307 |     0.00 |    0.02 |      0.00 | 7907.76 |      0.00
**  premdb  | public      | season      |    23177 |     0.00 |    0.02 |      0.00 |    0.11 |     14.68
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS storage_by_table_t CASCADE
;

CREATE TABLE storage_by_table_t
   (
      db_name     VARCHAR( 128 )
    , schema_name VARCHAR( 128 ) 
    , table_name  VARCHAR( 128 )    
    , table_id    BIGINT
    , rows_mil    NUMERIC( 18, 2 )
    , cmpr_gb     NUMERIC( 18, 2 )
    , uncmpr_gb   NUMERIC( 18, 2 )
    , db_gb       NUMERIC( 18, 2 )
    , pct_of_db   NUMERIC( 12, 2 )
   )
;
  

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE storage_by_table_p(
     _db_ilike VARCHAR DEFAULT '%'
   , _schema_ilike VARCHAR DEFAULT '%'
   , _table_ilike VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF storage_by_table_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'storage_by_table_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
BEGIN  

   --SET TRANSACTION       READ ONLY;
   
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* Join on db name is actually more effecient than cross join db_name at this point
   ** Valid only for YBD < 4.0
   */
   _sql := 'WITH  table_storage AS
        (  SELECT
              t.database_id               AS database_id
            , t.schema_id                 AS schema_id
            , t.table_id                  AS table_id       
            , t.name                      AS table_name
            , SUM (ts.rows_columnstore)   AS rows
            , SUM (ts.compressed_bytes)   AS cmpr_bytes
            , SUM (ts.uncompressed_bytes) AS uncmpr_bytes
           FROM
              sys.table              t
              JOIN sys.table_storage ts ON t.table_id    = ts.table_id
           WHERE
              ts.table_id > 16384
           GROUP BY
              1, 2, 3, 4
        )
        , db_storage AS
        (  SELECT
              database_id       AS database_id
            , SUM (cmpr_bytes)  AS cmpr_bytes
           FROM
              table_storage
           GROUP BY
              database_id
        )

      SELECT
         db_name::VARCHAR(128)
       , schema_name::VARCHAR(128)
       , table_name::VARCHAR(128)
       , table_id       
       , rows_mil::NUMERIC( 18, 2 )
       , cmpr_gb::NUMERIC( 18, 2 )
       , uncmpr_gb::NUMERIC( 18, 2 )
       , db_gb::NUMERIC( 18, 2 )
       , pct_of_db::NUMERIC( 12, 2 )
      FROM 
      (
         SELECT
            d.name::VARCHAR(128)                                     AS db_name
          , s.name::VARCHAR(128)                                     AS schema_name
          , ts.table_name                                            AS table_name
          , ts.table_id                                              AS table_id    
          , ROUND (ts.rows         / 1000000.0, 3) ::NUMERIC (18, 3) AS rows_mil
          , ROUND (ts.cmpr_bytes   / 1024.0^3, 3) ::NUMERIC (18, 3)  AS cmpr_gb
          , ROUND (ts.uncmpr_bytes / 1024.0^3, 3) ::NUMERIC (18, 3)  AS uncmpr_gb
          , ROUND (ds.cmpr_bytes   / 1024.0^3, 3) ::NUMERIC (18, 3)  AS db_gb
          , CASE WHEN db_gb = 0 THEN 0::NUMERIC
                 ELSE ROUND ( (cmpr_gb / db_gb) * 100.0, 2) ::NUMERIC (12, 3)  
            END                                                      AS pct_of_db
         FROM
            table_storage     ts
            JOIN db_storage   ds ON ts.database_id = ds.database_id
            JOIN sys.database d  ON ts.database_id = d.database_id
            JOIN sys.schema   s  ON ts.database_id = s.database_id  AND ts.schema_id = s.schema_id     
         WHERE  
               d.name        ILIKE ' || quote_literal( _db_ilike     ) || '
           AND s.name        ILIKE ' || quote_literal( _schema_ilike ) || '
           AND ts.table_name ILIKE ' || quote_literal( _table_ilike  ) || '
           AND ' || _yb_util_filter || '
      ) sbt
       ORDER BY db_name, schema_name, table_name
      '
      ;
   
   --RAISE INFO '_pred=%', _pred;
   RETURN QUERY EXECUTE _sql; 
  
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 

END;   
$proc$ 
;

-- ALTER FUNCTION storage_by_table_p( VARCHAR, VARCHAR, VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION storage_by_table_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
Storage summary for user tables by database, schema, and table.

Examples:
  SELECT * FROM storage_by_table_p( );
  SELECT * FROM storage_by_table_p( ''my_db'', ''s%'');
  SELECT * FROM storage_by_table_p( ''%'', ''%qtr%'' ,''%fact%'');  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the schema name. i.e. ''%fin%''.
                  The default is ''%''
. _schema_ilike - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                  The default is ''%''
. _table_ilike  - (optional) An ILIKE pattern for the table name.  i.e. ''fact%''.
                  The default is ''%''

Version:
. 2020.10.11 - Yellowbrick Technical Support 
. 2021.11.21 - Integrated with YbEasyCli 
'
;