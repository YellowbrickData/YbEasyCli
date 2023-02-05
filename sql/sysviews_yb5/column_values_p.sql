/* column_values_p.sql
**
** Column metadata for tables including row counts and value range.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 - 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever..
**
** Revision History:
** . 2022.10.09 - quote_ident( _db_name ) for dbnames that need to be double quoted.
** . 2022.08.28 - Added _yb_util_filter
** . 2022.04.27 - Remove views from output.
** . 2022.04.25 - Fix help description for procedure and remove addl debug output.
** . 2022.04.22 - Add col ilike and yb util filters.
** . 2022.04.06 - ybCliUtils inclusion.
** . 2021.07.11 - Yellowbrick Technical Support 
**
*/

/* ****************************************************************************
**  Example results:
**
 rel_id  | rel_kind |   db_name   | rel_schema | rel_name | col_id | col_name |   col_type   | nullable | encrypted 
---------+----------+-------------+------------+----------+--------+----------+--------------+----------+-----------
 1183390 | r        | yellowbrick | public     | a_char   |      1 | c1       | char(1)      | t        |           
 1183529 | r        | yellowbrick | public     | a_vchar  |      1 | c1       | varchar(256) | t        |           
 3366587 | r        | yellowbrick | public     | ac_date  |      1 | c1       | date         | t        |           
 1142241 | r        | yellowbrick | public     | c        |      1 | c1       | char(1000)   | t        |           
 1197971 | r        | yellowbrick | public     | c16      |      1 | c1       | char(16)     | t        |           
** ... 
** ... |  min_val   |     max_val      | num_vals | num_rows | owner_name
** ... +------------+------------------+----------+----------+-------------
** ... | a          | z                |        3 |        3 | yellowbrick
** ... | aaa        | zzzyzxyzzzzyzxyz |    53568 |    53608 | yellowbrick
** ... | 2022-10-05 | 2043-04-09       |   240024 |   240024 | yellowbrick
** ... | one        | two              |        3 |        3 | yellowbrick
** ... |            |                  |        0 |        0 | yellowbrick
*/

/* ****************************************************************************
** Create a table to define the schema for the resulting temp table.
*/
DROP   TABLE IF EXISTS column_values_t CASCADE;
CREATE TABLE           column_values_t
   (
      rel_id       BIGINT
    , rel_kind     CHAR(1)    
    , db_name      VARCHAR( 128 )
    , rel_schema   VARCHAR( 128 )    
    , rel_name     VARCHAR( 128 )
    , col_id       SMALLINT
    , col_name     VARCHAR( 128 )
    , col_type     VARCHAR(  24 )
    , nullable     BOOLEAN
    , encrypted    BOOLEAN
    , min_val      VARCHAR( 60000 )
    , max_val      VARCHAR( 60000 )
    , num_vals     BIGINT    
    , num_rows     BIGINT
    , owner_name   VARCHAR(128)
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE column_values_p( _db_ilike       VARCHAR DEFAULT '%'
                                           , _schema_ilike   VARCHAR DEFAULT '%'
                                           , _table_ilike    VARCHAR DEFAULT '%'
                                           , _column_ilike   VARCHAR DEFAULT '%'
                                           , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
                                           )
   RETURNS SETOF column_values_t 
   LANGUAGE PLPGSQL
   SECURITY DEFINER
AS
$proc$
DECLARE

   _ts             TIMESTAMP := now();
   _delim          CHAR( 1 ) := ' ';
   _rows           INTEGER   := 0;
                   
   _db_id          BIGINT    := 0;
   _db_name        VARCHAR   := '';   
   _cols_sql       TEXT      := '';
   _col_vals_sql   TEXT      := '';   
                   
   _db_rec         RECORD;
   _col_rec        column_values_t%rowtype;
   _col_vals_rec   RECORD;

   _pred           TEXT := '';
   _dbs_sql        TEXT := '';

   _fn_name   VARCHAR(256) := 'column_values_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
   _ret_rec column_values_t%ROWTYPE;   
  
BEGIN  
  
   -- Append sysviews:proc_name to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _tags );
      
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);
   
   -- Query for the databases to iterate over
   _dbs_sql = 'SELECT database_id AS db_id, name AS db_name 
      FROM sys.database 
      WHERE name ILIKE (' || quote_literal (_db_ilike) || ') 
      ORDER BY name
   ' ;
   -- RAISE info 'db list sql = %', _dbs_sql;

   /* Iterate over each db and get the relation metadata including schema 
   */
   FOR _db_rec IN EXECUTE _dbs_sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      --RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ;

      _cols_sql := 'WITH owners AS
      (  SELECT user_id AS owner_id
          , name        AS name
         FROM sys.user
         UNION ALL
         SELECT role_id AS owner_id
          , name        AS name
         FROM sys.role
      )
   
      SELECT
        c.oid::BIGINT                                                             AS rel_id    
      , c.relkind::CHAR(1)                                                        AS rel_kind  
      ,' || quote_literal( _db_name ) || '::VARCHAR(128)                          AS db_name      
      , trim( n.nspname )::VARCHAR(128)                                           AS rel_schema
      , trim( c.relname )::VARCHAR(128)                                           AS rel_name
      , a.attnum::SMALLINT                                                        AS col_id
      , trim( a.attname )::VARCHAR(128)                                           AS col_name
      , CASE 
            WHEN t.typname = ''bpchar''  THEN ''char(''    || a.atttypmod -4 || '')''
            WHEN t.typname = ''varchar'' THEN ''varchar('' || a.atttypmod -4 || '')''
            WHEN t.typname = ''numeric'' THEN ''numeric('' || (((atttypmod - 4) >> 16) & 65535) 
                                                  || '','' || (a.atttypmod%65536 -4) || '')''                                       
            ELSE t.typname 
        END::VARCHAR(24)                                                          AS col_type
      , NOT a.attnotnull                                                          AS nullable
      , (CASE WHEN e.cerelid IS NOT NULL THEN ''y'' ELSE NULL END)::BOOLEAN       AS encrypted
      , NULL::VARCHAR( 60000 )                                                    AS min_val      
      , NULL::VARCHAR( 60000 )                                                    AS max_val
      , NULL::BIGINT                                                              AS num_vals
      , NULL::BIGINT                                                              AS num_rows    
      , o.name::VARCHAR( 128 )                                                    AS owner_name
      FROM
           ' || quote_ident( _db_name ) || '.pg_catalog.pg_class             c  
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_namespace         n   ON c.relnamespace = n.oid
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_attribute         a   ON c.oid          = a.attrelid
      JOIN ' || quote_ident( _db_name ) || '.pg_catalog.pg_type              t   ON a.atttypid     = t.oid    
      LEFT OUTER JOIN owners                                  o   ON c.relowner     = o.owner_id                
      LEFT OUTER JOIN 
           ' || quote_ident( _db_name ) || '.pg_catalog.pg_column_encryption e   ON ((e.cerelid = a.attrelid) AND (e.cenum = a.attnum))
      WHERE
          c.relname ILIKE ' || quote_literal( _table_ilike  ) || '
      AND n.nspname ILIKE ' || quote_literal( _schema_ilike ) || '
      AND a.attname ILIKE ' || quote_literal( _column_ilike ) || '
      AND a.attnum > 0         
      AND c.relkind IN ( ''r'' )   
      AND c.oid > 16384
      AND ' || _yb_util_filter || '      
      ORDER BY db_name, rel_schema, rel_name, col_name
      ';

      --RAISE INFO '_cols_sql = %', _cols_sql ;
      FOR _col_rec IN EXECUTE _cols_sql 
      LOOP      
         _col_vals_sql := 'SELECT 
            MIN('   || quote_ident( _col_rec.col_name ) || ')::VARCHAR(60000) AS min_val
          , MAX('   || quote_ident( _col_rec.col_name ) || ')::VARCHAR(60000) AS max_val
          , COUNT(' || quote_ident( _col_rec.col_name ) || ') AS num_vals
          , COUNT(*)                                          AS num_rows
         FROM ' || quote_ident( _db_name ) || '.' 
                || quote_ident(_col_rec.rel_schema) || '.' 
                || quote_ident(_col_rec.rel_name) ;
            
         FOR _col_vals_rec IN EXECUTE _col_vals_sql 
         LOOP 
            --RAISE INFO '.';
            _col_rec.min_val  := _col_vals_rec.min_val;
            _col_rec.max_val  := _col_vals_rec.max_val;
            _col_rec.num_vals := _col_vals_rec.num_vals;
            _col_rec.num_rows := _col_vals_rec.num_rows;
         END LOOP;
         
         RETURN NEXT _col_rec;
         
      END LOOP;
      
   END LOOP;
   
   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );
   
END;
$proc$
;


COMMENT ON FUNCTION column_values_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
Column metadata including row counts, and min and max values.

Note:
This procedure results in a full table scan for all specified columns. This can be 
very expensive from an IO and time perspective.

Examples:
  SELECT * FROM column_values_p( 'my_db');
  SELECT * FROM column_values_p( 'my_db', 's%');
  SELECT * FROM column_values_p( 'my_db', '%' ,'%fact%');  
  SELECT * FROM column_values_p( '%', '%' ,'%fact%', 'rate%');    
  
Arguments:
. _db_ilike       - (optional) An ILIKE pattern for the schema name. i.e. 'finance'.
. _schema_ilike   - (optional) An ILIKE pattern for the schema name. i.e. '%qtr%'.
. _table_ilike    - (optional) An ILIKE pattern for the table name.  i.e. 'fact%'.
. _column_ilike   - (optional) An ILIKE pattern for the column name. i.e. 'rate%'.
. _yb_util_filter - (internal) Used by YB CLI Utils.

Version:
. 2022.10.09 - Yellowbrick Technical Support  
$cmnt$
;


