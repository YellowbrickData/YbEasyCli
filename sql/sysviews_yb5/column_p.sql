/* column_p.sql
**
** Cross-database column metadata for tables and views similar to \d.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2021 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever..
**
** Revision History:
** . 2022.12.28 - Fix procedure COMMENT ON.
**                Rename rel_type and schema_name columns.
** . 2022.10.09 - quote_ident( _db_name ) for dbnames that need to be double quoted.
** . 2022.04.21 - Add col ilike and yb util filters.
** . 2022.04.06 - ybCliUtils inclusion.
** . 2021.07.11 - Yellowbrick Technical Support 
**
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | rel_id | rel_type | schema_name | rel_name | col_id |        col_name        | col_type | nullable | encrypted
** -------------+--------+----------+-------------+----------+--------+------------------------+----------+----------+-----------
**  yellowbrick |  17722 | table    | public      | dly      |      1 | pxdn_za_pc             | char(8)  | t        |
**  yellowbrick |  17722 | table    | public      | dly      |      2 | pxdn_mc_pc             | char(8)  | t        |
**  yellowbrick |  17722 | table    | public      | dly      |      3 | prlt_xfer_to_prod_cd   | char(8)  | t        |
**  yellowbrick |  17722 | table    | public      | dly      |      4 | prlt_xfer_to_cntl_prod | char(8)  | t        |
**  yellowbrick |  18057 | view     | public      | dly_v    |      1 | pxdn_za_pc             | char(8)  | t        |
**  yellowbrick |  18057 | view     | public      | dly_v    |      2 | pxdn_mc_pc             | char(8)  | t        |
**  yellowbrick |  18057 | view     | public      | dly_v    |      3 | prlt_xfer_to_prod_cd   | char(8)  | t        |
**  yellowbrick |  18057 | view     | public      | dly_v    |      4 | prlt_xfer_to_cntl_prod | char(8)  | t        |
** ... 
*/

/* ****************************************************************************
** Create a table to define the schema for the resulting temp table.
*/
DROP   TABLE IF EXISTS column_t CASCADE;
CREATE TABLE           column_t
   (
      db_name      VARCHAR( 128 )
    , rel_id       BIGINT
    , rel_type     VARCHAR(5)    
    , schema_name  VARCHAR( 128 )    
    , rel_name     VARCHAR( 128 )
    , col_id       SMALLINT
    , col_name     VARCHAR( 128 )
    , col_type     VARCHAR(  24 )
    , nullable     BOOLEAN
    , encrypted    BOOLEAN
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE column_p( _db_ilike       VARCHAR DEFAULT '%'
                                    , _schema_ilike   VARCHAR DEFAULT '%'
                                    , _table_ilike    VARCHAR DEFAULT '%'
                                    , _column_ilike   VARCHAR DEFAULT '%'
                                    , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
                                    )
   RETURNS SETOF column_t 
   LANGUAGE PLPGSQL
   SECURITY DEFINER
AS
$proc$
DECLARE

   _ts           TIMESTAMP := now();
   _tmp_tbl_name TEXT      := 'temp_column_info';
   _delim        CHAR( 1 ) := ' ';
   _rows         INTEGER   := 0;

   _db_id        BIGINT    := 0;
   _db_name      VARCHAR   := '';   
   _cols_sql     TEXT      := '';
   _col_info_sql TEXT      := '';   
   
   _db_rec       RECORD;
   _col_rec      RECORD;

   _pred         TEXT := '';
   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'column_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
   _ret_rec column_t%ROWTYPE;   
  
BEGIN  
  
   -- Append sysviews:proc_name to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _tags );
   
   -- Query for the databases to iterate over
   _sql = 'SELECT database_id AS db_id, name AS db_name 
      FROM sys.database 
      WHERE name ILIKE (' || quote_literal (_db_ilike) || ') 
      ORDER BY name
   ' ;
   --RAISE info 'db list sql = %', _sql;

   -- Iterate over each db and get the relation metadata including schema 
   FOR _db_rec IN EXECUTE _sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      --RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ;

      _cols_sql := 'SELECT
      ' || quote_literal( _db_name ) || '::VARCHAR(128)                           AS db_name
      , c.oid::BIGINT                                                             AS rel_id    
      , (CASE WHEN c.relkind=''r'' THEN ''table'' ELSE ''view'' END)::VARCHAR(5)  AS rel_type  
      , trim( n.nspname )::VARCHAR(128)                                           AS schema_name
      , trim( c.relname )::VARCHAR(128)                                           AS rel_name
      , a.attnum::SMALLINT                                                        AS col
      , trim( a.attname )::VARCHAR(128)                                           AS col_name
      , CASE 
            WHEN pt.typname = ''bpchar''  THEN ''char(''    || a.atttypmod -4 || '')''
            WHEN pt.typname = ''varchar'' THEN ''varchar('' || a.atttypmod -4 || '')''
            WHEN pt.typname = ''numeric'' THEN ''numeric('' || (((atttypmod - 4) >> 16) & 65535) 
                                                   || '','' || (a.atttypmod%65536 -4) || '')''                                       
         ELSE pt.typname 
        END::VARCHAR(24)                                                         AS col_type
      , NOT a.attnotnull                                                         AS nullable
      , (CASE WHEN e.cerelid IS NOT NULL THEN ''y'' ELSE NULL END)::BOOLEAN      AS encrypted     
      FROM
           ' || quote_ident( _db_name ) || '.pg_catalog.pg_class             AS c  
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_namespace         AS n   ON c.relnamespace = n.oid
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_attribute         AS a   ON c.oid          = a.attrelid
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_type              AS pt  ON a.atttypid     = pt.oid            
      LEFT OUTER JOIN 
           ' || quote_ident( _db_name ) || '.pg_catalog.pg_column_encryption AS e   ON ((e.cerelid = a.attrelid) AND (e.cenum = a.attnum))
      WHERE
          c.relname ILIKE ' || quote_literal( _table_ilike  ) || '
      AND n.nspname ILIKE ' || quote_literal( _schema_ilike ) || '
      AND a.attname ILIKE ' || quote_literal( _column_ilike ) || '
      AND a.attnum > 0         
      AND c.relkind IN ( ''r'', ''v'' )   
      AND c.oid > 16384
      AND ' || _yb_util_filter || '      
      ORDER BY db_name, schema_name, rel_name, col
      ';

      --RAISE INFO '_cols_sql = %', _cols_sql ;
      RETURN QUERY  EXECUTE ( _cols_sql );
      
   END LOOP;
   
   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );
   
END;
$proc$
;


COMMENT ON PROCEDURE column_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
Table column information similar to "\d" but with additional metadata and for 
both tables and views.

Examples:
  SELECT * FROM column_p( 'my_db');
  SELECT * FROM column_p( 'my_db', 's%');
  SELECT * FROM column_p( '%y%', '%' ,'%fact%');  
  SELECT * FROM column_p( '%', '%' ,'%fact%', 'rate%');    
  
Arguments:
. _db_ilike       - (optional) An ILIKE pattern for the database name. i.e. 'finance'.
. _schema_ilike   - (optional) An ILIKE pattern for the schema name. i.e. '%qtr%'.
. _table_ilike    - (optional) An ILIKE pattern for the table name.  i.e. 'fact%'.
. _column_ilike   - (optional) An ILIKE pattern for the column name. i.e. 'rate%'.
. _yb_util_filter - (internal) Used by YB Utils.

Version:
. 2022.12.28 - Yellowbrick Technical Support  
$cmnt$
;




