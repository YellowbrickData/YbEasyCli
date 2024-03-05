/* ****************************************************************************
** catalog_storage_by_table_p.sql
**
** Storage space of catalog tables.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2022.12.27 - YbEasyCli inclusion.
** . 2022.07.07 - Yellowbrick Technical Support  
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | table_id | schema_name |         table_name         | row_est | total_size | table_size | index_size | toast_size
** -------------+----------+-------------+----------------------------+---------+------------+------------+------------+------------
**  yellowbrick |     9901 | sys         | yb_query_execution_analyze |  444734 | 401 MB     | 391 MB     | 10016 kB   | 32 kB
**  yellowbrick |     9927 | sys         | yb_query_plan              | 1810851 | 377 MB     | 234 MB     | 142 MB     |
**  yellowbrick |     5003 | sys         | shardstore                 |  313328 | 176 MB     | 87 MB      | 89 MB      |
**  yellowbrick |     4259 | pg_catalog  | yb_deletes_pg_shdepend     | 1170968 | 144 MB     | 116 MB     | 28 MB      |
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS catalog_storage_by_table_t CASCADE
;

CREATE TABLE catalog_storage_by_table_t
(
   db_name        VARCHAR(128)     
 , table_id       BIGINT      
 , schema_name    VARCHAR(128)              
 , table_name     VARCHAR(128)   
 , row_est        BIGINT 
 , total_size     VARCHAR(32)                             
 , table_size     VARCHAR(32)                               
 , index_size     VARCHAR(32)                                
 , toast_size     VARCHAR(32)                               
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE catalog_storage_by_table_p( 
      _db_ilike       VARCHAR DEFAULT '%'
    , _schema_ilike   VARCHAR DEFAULT '%'
    , _table_ilike    VARCHAR DEFAULT '%'
    , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
   )
   RETURNS SETOF catalog_storage_by_table_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _debug     INTEGER      := 0;
   _db_id     BIGINT;
   _db_name   VARCHAR( 128 );   
   _db_rec    RECORD;  
   _dbs_sql   TEXT         := '';
   _rec       RECORD;
   _ret_rec   catalog_storage_by_table_t%ROWTYPE;   
   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'catalog_storage_by_table_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags );
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);   
   
   -- Get the list of databases to iterate over.
   _dbs_sql = 'SELECT 
      database_id AS db_id
    , name        AS db_name 
   FROM sys.database 
   WHERE name ILIKE ' || quote_literal( _db_ilike ) || ' 
   ORDER BY name
   ';      
   
   IF ( _debug > 0 ) THEN RAISE INFO '_dbs_sql=%', _dbs_sql; END IF;
     

   -- Iterate over each db and get the table sizes and metadata. 
   FOR _db_rec IN EXECUTE _dbs_sql 
   LOOP
   
      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;
      IF ( _debug > 0 ) THEN RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ; END IF;   

      _sql := 'SELECT
   ' || quote_literal(_db_name) || '::VARCHAR(128) AS db_name
      , table_id::BIGINT                           AS table_id
      , table_schema::VARCHAR(128)                 AS table_schema
      , table_name::VARCHAR(128)                   AS table_name
      , row_estimate::BIGINT                       AS row_est 
      , pg_size_pretty(total_bytes)::VARCHAR(32)   AS total_size
      , pg_size_pretty(table_bytes)::VARCHAR(32)   AS table_size
      , pg_size_pretty(index_bytes)::VARCHAR(32)   AS index_size
      , pg_size_pretty(toast_bytes)::VARCHAR(32)   AS toast_size
      FROM
        ( SELECT *
          , total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
          FROM
            ( SELECT
           ' || quote_literal(_db_name) || '::VARCHAR(128) AS db_name
              , c.oid                                      AS table_id
              , nspname                                    AS table_schema
              , relname                                    AS table_name
              , c.reltuples                                AS row_estimate
              , pg_total_relation_size(c.oid)              AS total_bytes
              , pg_indexes_size(c.oid)                     AS index_bytes
              , pg_total_relation_size(reltoastrelid)      AS toast_bytes
              , CASE WHEN (n.nspname = ''sys'' OR table_name = ''yb_deletes_pg_shdepend'')
                          AND db_name != ''yellowbrick''
                     THEN ''f''
                     ELSE ''t''
                END::BOOLEAN                               AS to_display
              FROM
            ' || quote_ident(_db_name) || '.pg_catalog.pg_class                   c
                LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace n
                  ON n.oid = c.relnamespace
              WHERE
                relkind = ''r''
                AND total_bytes > 0
                AND c.relname ILIKE ' || quote_literal(_table_ilike)  || '
                AND n.nspname ILIKE ' || quote_literal(_schema_ilike) || '
                AND to_display = ''t''
                AND nspname NOT IN ( ''information_schema'')                
            ) pc
        )     a
      WHERE ' || _yb_util_filter || '
      ORDER BY
        total_bytes DESC 
      ';

      RETURN QUERY EXECUTE _sql; 
   
   END LOOP;   

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _prev_tags ); 
   
END;   
$proc$ 
;


COMMENT ON FUNCTION catalog_storage_by_table_p(VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
$cmnt$Description:
Size of catalog tables across all databases.
  
Examples:
  SELECT * FROM catalog_storage_by_table_p() WHERE total_size ~ '(GB|\d{3} MB)';
  SELECT * FROM catalog_storage_by_table_p( 'yellowbrick', 'sys');
  SELECT * FROM catalog_storage_by_table_p( 'yellowbrick', 'sys', '%log%' );
  
Arguments:
. _db_ilike       - (optional) An ILIKE pattern for the database name. i.e. '%dim%'.
. _schema_ilike   - (optional) An ILIKE pattern for the schema name. i.e. 'sys'.
. _table_ilike    - (optional) An ILIKE pattern for the table name.  i.e. 'log%'.
. _yb_util_filter - (internal) Used by YbEasyCli.

Notes:
. The sys schema and pg_catalog.yb_deletes_pg_shdepend table are global and so
  are reported only under the yellowbrick database.
. Sizes are pretty printed so are text, not numeric.  

Example Result:
   db_name   | table_id | schema_name |  table_name   | row_est | total_size | table_size | index_size | toast_size
-------------+----------+-------------+---------------+---------+------------+------------+------------+------------
 yellowbrick |     9927 | sys         | yb_query_plan | 4090043 | 832 MB     | 467 MB     | 364 MB     |
 yellowbrick |     5003 | sys         | shardstore    |  195884 | 313 MB     | 155 MB     | 158 MB     |
 
Version:
. 2022.12.27 - Yellowbrick Technical Support
$cmnt$
;

