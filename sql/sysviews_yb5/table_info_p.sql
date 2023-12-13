/* ****************************************************************************
** table_info_p()
**
** Table metadata including owner, rows, key attributes, constraints, storage space.
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
** TODO: Fix skew pct calc
**
** Revision History:
** . 2023.12.08 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example result:
** 
**  table_id |   db_name   | schema_name |   table_name    |  rows  | cmpr_mb | uncmpr_mb | skew_pct |     dist      | sort_key | clstr_keys | prtn_keys |        pkey        | uniqs | fkeys | owner_name
** ----------+-------------+-------------+-----------------+--------+---------+-----------+----------+---------------+----------+------------+-----------+--------------------+-------+-------+-------------
**   1127721 | yellowbrick | public      | clientorders    | 262144 |      32 |        18 |      2.0 | hash(clordid) | loaddate |            |           | pk_clientorders    |       |       | yellowbrick
**   1127727 | yellowbrick | public      | clientorders_iq |      0 |       0 |         0 |    100.0 | hash(clordid) | loaddate |            |           | pk_clientorders_iq |       |       | yellowbrick
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS table_info_t CASCADE
;

CREATE TABLE table_info_t
   (
      table_id        BIGINT
    , db_name         VARCHAR(   128 )
    , schema_name     VARCHAR(   128 ) 
    , table_name      VARCHAR(   128 )    
    , rows            BIGINT
    , cmpr_mb         BIGINT
    , uncmpr_mb       BIGINT
    , skew_pct        NUMERIC( 19, 1 )
    , dist            VARCHAR(   140 )    
    , sort_key        VARCHAR(   128 )    
    , clstr_keys      VARCHAR(   512 )    
    , prtn_keys       VARCHAR(   512 )  
    , pkey            VARCHAR(  8192 )
    , uniqs           VARCHAR(  8192 )
    , fkeys           VARCHAR(  8192 )
    , owner_name      VARCHAR(   128 )     
   )
;
  

/* ****************************************************************************
** Create the procedure.
** 
** Note on contraint types: 
** 'c' => 'CHECK' 
** 'f' => 'FOREIGN KEY' 
** 'p' => 'PRIMARY KEY' 
** 'u' => 'UNIQUE' 
**
*/
CREATE PROCEDURE table_info_p(  _db_ilike       VARCHAR DEFAULT '%'
                              , _schema_ilike   VARCHAR DEFAULT '%'
                              , _table_ilike    VARCHAR DEFAULT '%'
                              , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
                           )
RETURNS SETOF table_info_t 
LANGUAGE 'plpgsql' 
VOLATILE
CALLED ON NULL INPUT
SECURITY DEFINER
AS 
$proc$
DECLARE

   _ts           TIMESTAMP := now();
   _delim        CHAR( 1 ) := ' ';
   _rows         INTEGER   := 0;

   _db_sql       TEXT := '';
   _db_rec       RECORD;
   _db_id        BIGINT    := 0;
   _db_name      VARCHAR   := '';   
   
   _rel_sql      TEXT := '';
   _rel_rec      RECORD;
      
   _cols_sql     TEXT      := '';
   _col_info_sql TEXT      := '';   
   _col_rec      RECORD;
-- _col_info_rec column_t%rowtype;
   
   _pred         TEXT := '';
   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'table_info_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
   _ret_rec table_info_t%ROWTYPE;   
  
BEGIN  

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';  
   --PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* ****************************************************************************
   ** Iterate over each db and get the relation metadata
   */
   _db_sql := 'SELECT 
         database_id AS db_id
       , name        AS db_name 
      FROM sys.database 
      WHERE name ILIKE (' || quote_literal (_db_ilike) || ') 
      ORDER BY name
   ' ;
   -- RAISE INFO '_db_sql = %', _db_sql;

   FOR _db_rec IN EXECUTE _db_sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      --RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ;
      /* Currently we are querying only for user tables so using sys.table.
      */
            
      _rel_sql := 'WITH 
      schemas AS
      (  SELECT 
            oid::BIGINT           AS oid
          , nspname::VARCHAR(128) AS nspname
         FROM ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace
         WHERE nspname ILIKE ' || quote_literal( _schema_ilike ) || '
      )  
      , tables AS
      (
         SELECT
           t.database_id                                                    AS database_id
         , t.schema_id                                                      AS schema_id
         , t.table_id                                                       AS table_id       
         , t.name                                                           AS name
         , s.nspname                                                        AS schema_name
         , t.owner_id                                                       AS owner_id
         , CASE
           WHEN t.distribution <> ''hash'' THEN t.distribution
           ELSE t.distribution || ''('' || t.distribution_key || '')''
         END                                                                AS dist
         , t.sort_key                                                       AS sort_key
         , t.cluster_keys                                                   AS cluster_keys
         , t.partition_keys                                                 AS partition_keys      
         , NVL(t.rowstore_row_count, 0)                                     AS rowstore_row_count
         , NVL(t.rowstore_bytes, 0)                                         AS rowstore_bytes         
         FROM sys.table      AS t 
         JOIN schemas        AS s  ON t.schema_id = s.oid
         WHERE t.database_id = ' || _db_id || ' 
           AND t.name  ILIKE ' || quote_literal(_table_ilike)  || '
      ) 
      , worker_storage AS
      (  SELECT 
            table_id                  AS table_id
          , COUNT(DISTINCT worker_id) AS wrkrs         
          , SUM(rows_columnstore)     AS rows 
          , MAX(rows_columnstore)     AS max_rows
          , SUM(compressed_bytes)     AS cmpr_bytes
          , SUM(uncompressed_bytes)   AS uncmpr_bytes
         FROM sys.table_storage 
         GROUP BY 1
      ) 
      , table_info AS
      (
         SELECT
           t.database_id                                                    AS database_id
         , t.schema_id                                                      AS schema_id
         , t.table_id                                                       AS table_id       
         , t.name                                                           AS table_name
         , t.schema_name                                                    AS schema_name
         , t.owner_id                                                       AS owner_id
         , t.dist                                                           AS dist
         , t.sort_key                                                       AS sort_key
         , t.cluster_keys                                                   AS clstr_keys
         , t.partition_keys                                                 AS prtn_keys      
         , NVL(t.rowstore_row_count, 0) + ws.rows                           AS rows
         , ws.max_rows                                                      AS max_rows
         , ROUND((NVL(t.rowstore_bytes, 0) + ws.cmpr_bytes  )/1024.0^2,1)   AS cmpr_mb         
         , ROUND((NVL(t.rowstore_bytes, 0) + ws.uncmpr_bytes)/1024.0^2,1)   AS uncmpr_mb
         , ws.wrkrs                                                         AS wrkrs
         FROM tables          AS t
         JOIN worker_storage  AS ws ON t.table_id  = ws.table_id
      ) 
      , pkeys AS 
      (  SELECT 
            conrelid
          , conname 
         FROM ' || quote_ident( _db_name ) || '.pg_catalog.pg_constraint
         WHERE contype = ''p''::CHAR
           AND conrelid > 16383
      )
      , uniqs AS 
      (  SELECT 
            conrelid                            as conrelid
          , string_agg(distinct conname, '','') as connames 
         FROM ' || quote_ident( _db_name ) || '.pg_catalog.pg_constraint c
         JOIN tables AS t ON c.conrelid = t.table_id
         WHERE contype  = ''u''::CHAR
         GROUP BY conrelid
      )
      , fkeys AS 
      (  SELECT 
            conrelid                            as conrelid
          , string_agg(distinct conname, '','') as connames 
         FROM ' || quote_ident( _db_name ) || '.pg_catalog.pg_constraint c
         JOIN tables AS t ON c.conrelid = t.table_id
         WHERE contype  = ''f''::CHAR
         GROUP BY conrelid
      )
      , owners AS
      (  SELECT 
          ''USER''       AS owner_type
          , user_id      AS owner_id
          , name         AS owner_name
         FROM sys.user
         UNION ALL
         SELECT 
          ''ROLE''       AS owner_type
          , role_id      AS owner_id
          , name         AS owner_name
         FROM sys.role
      )
      
      SELECT 
         ti.table_id::BIGINT            AS table_id
       ,'|| quote_literal(_db_name) || '::VARCHAR( 128 ) 
                                        AS db_name
       , ti.schema_name::VARCHAR( 128 ) AS schema_name
       , ti.table_name::VARCHAR( 128 )  AS table_name
       , ti.rows::BIGINT                AS rows
       , ti.cmpr_mb::BIGINT             AS cmpr_mb
       , ti.uncmpr_mb::BIGINT           AS uncmpr_mb
      , IIF((ti.rows-ti.max_rows)=0 OR ti.wrkrs=0
             , 100
             , ROUND((ti.max_rows/(ti.rows/ti.wrkrs::FLOAT8)-1)*100,1)
            )::NUMERIC(19,1)            AS skew_pct       
       , dist::VARCHAR( 140 )           AS dist    
       , sort_key::VARCHAR( 128 )       AS sort_key    
       , clstr_keys::VARCHAR( 512 )     AS clstr_keys    
       , prtn_keys::VARCHAR( 512 )      AS prtn_keys     
       , pk.conname::VARCHAR( 8192 )    AS pkey
       , u.connames::VARCHAR( 8192 )    AS uniqs
       , NULL::VARCHAR( 8192 )          AS fkeys
       , o.owner_name::VARCHAR( 128 )   AS owner_name    
       
      FROM table_info  AS ti
      LEFT JOIN pkeys  AS pk ON ti.table_id = pk.conrelid
      LEFT JOIN uniqs  AS u  ON ti.table_id = u.conrelid
      LEFT JOIN fkeys  AS fk ON ti.table_id = fk.conrelid
      LEFT JOIN owners AS o  ON ti.owner_id = o.owner_id
      ORDER BY db_name, schema_name, table_name
      ';
      
  
      --RAISE INFO '_rel_sql = %', _rel_sql;
      RETURN QUERY EXECUTE _rel_sql ;
   
   END LOOP;
   
   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;   
$proc$ 
;


COMMENT ON FUNCTION table_info_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
$cmnt$Description:
Table metadata including owner, rows, key attributes, constraints, and storage space.

Useful in table design & query evaluation.

Examples:
  SELECT * FROM rel_ddl_p( );
  SELECT * FROM rel_ddl_p( 'my_db', 'public');
  SELECT * FROM rel_ddl_p( '%fin%', 'qtr%' ,'%fact');  
  
Arguments:
. _db_ilike     VARCHAR (optl) - An ILIKE pattern for the schema name. i.e. '%fin%'.
                                 The default is '%'
. _schema_ilike VARCHAR (optl) - An ILIKE pattern for the schema name. i.e. 'public'.
                                 The default is '%'
. _table_ilike  VARCHAR (optl) - An ILIKE pattern for the table name.  i.e. 'fact%'.
                                 The default is '%'

Notes:
. skew  is based on row count % of max > average.
. pkey  is the primary key constraint name, if it exists.
. uniqs is a string with the unique constraint names if any.
. fkeys is a string with the foreign key constraint names if any.

Version:
. 2023.12.08 - Yellowbrick Technical Support 
$cmnt$
;
