/* ****************************************************************************
** rel_ddl_p()
**
** User table metadata similar to sys.table. 
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
** . 2022.12.02 - added additional columns. 
**                      db, schema, and table ilike args
** . 2022.06.06 - ybCliUtils inclusion. 
** . 2021.05.08 - Yellowbrick Technical Support 
** . 2020.11.09 - Yellowbrick Technical Support 
** . 2020.10.30 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example result:
** 
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP   TABLE IF EXISTS rel_ddl_t CASCADE
;
CREATE TABLE           rel_ddl_t (line varchar(64000))
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE rel_ddl_p(  _db_ilike       VARCHAR DEFAULT '%'
                           , _schema_ilike   VARCHAR DEFAULT '%'
                           , _table_ilike    VARCHAR DEFAULT '%'
                           , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
                          )
   RETURNS SETOF rel_ddl_t 
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
   _col_info_rec column_t%rowtype;
   

   _pred         TEXT := '';
   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'rel_ddl_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
   _ret_rec rel_ddl_t%ROWTYPE;   
  
BEGIN  

   -- Append sysviews:proc_name to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';  
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

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
      relations AS
      (
         SELECT
           database_id                                                   AS db_id
         , table_id                                                      AS table_id           
         , schema_id                                                     AS schema_id     
         , name                                                          AS table_name
         , owner_id                                                      AS owner_id   
         , ''DISTRIBUTE '' 
         || CASE
            WHEN distribution = ''hash''      THEN ''ON ('' || distribution_key || '')''
            WHEN distribution = ''replicate'' THEN ''REPLICATED''
            ELSE UPPER(distribution)    
           END                                                           AS distribution
         , CASE 
            WHEN sort_key     IS NOT NULL AND sort_key     != '''' THEN ''SORT    ON ('' || sort_key     || '')''  ' || '   
            WHEN cluster_keys IS NOT NULL AND cluster_keys != '''' THEN ''CLUSTER ON ('' || cluster_keys || '')''  ' || '
            ELSE NULL::VARCHAR
           END                                                           AS sort_or_clstr_on
         , partition_keys                                                AS prnt_on      
         , creation_time                                                 AS creation_time
         FROM sys.table 
         WHERE  -- Only regular tables
                table_id  > 16384 
            AND table_name ILIKE ' || quote_literal( _table_ilike  ) || '         
      )
      
      , schemas AS
      (  SELECT 
            oid::BIGINT           AS oid
          , nspname::VARCHAR(128) AS nspname
         FROM ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace
         WHERE nspname ILIKE ' || quote_literal( _schema_ilike ) || '
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
         r.table_id                                                      AS table_id
       , quote_ident(n.nspname) || ''.'' || quote_ident(r.table_name)    AS qtn
       , r.distribution                                                  AS distribution
       , r.sort_or_clstr_on                                              AS sort_or_clstr_on
       , r.prnt_on                                                       AS prnt_on  
       , o.owner_name                                                    AS owner_name
      FROM relations      r
         JOIN schemas     n ON r.schema_id = n.oid
         JOIN owners      o ON r.owner_id  = o.owner_id
      WHERE 
         r.db_id = ' || _db_id  || '
      ';
  
      --RAISE INFO '_rel_sql = %', _rel_sql;
      
      /* ****************************************************************************
       * All user columns in qualifying user relations (tables only at this point) 
       * in the selected db. i.e.
       *
       *  table_id | column_id |  name    |     type      | nullable | distribution_key | sort_key | partition_key | cluster_key | encrypted
       * ----------+-----------+----------+---------------+----------+------------------+----------+---------------+-------------+-----------
       *   6479717 |         8 | ratio    | numeric(16,1) | t        |                  |          |               |             | f
       *   5465563 |         4 | secs_pct | numeric(38,2) | t        |                  |          |               |             | f       
       * 
       * yellowbrick=# select * from pg_catalog.pg_column_encryption;
       *  cerelid | cenum | encryption_type | encryption_algorithm | key_database | key_namespace | keyname
       * ---------+-------+-----------------+----------------------+--------------+---------------+----------
       *  6616019 |     2 | deterministic   | aes_256_ofb          |         4400 |          2200 | key_abcd
       * (1 row)
       */
       
      FOR _rel_rec IN EXECUTE _rel_sql 
      LOOP

         RETURN QUERY  EXECUTE $$SELECT ('CREATE TABLE $$ || _rel_rec.qtn || CHR(10) || $$(')::VARCHAR(64000) $$;
         
         _cols_sql := 'SELECT
         (   CASE WHEN c.column_id = 1     THEN ''   ''           ELSE '' , '' END  
          || name 
          || '' '' || UPPER( c.type )
          || CASE WHEN c.nullable = ''f''  THEN '' NOT NULL''     ELSE ''''    END 
          || CASE WHEN e.cenum IS NOT NULL THEN '' -- Encrypted'' ELSE ''''    END  
         )::VARCHAR(64000)                                                         AS delim  
         FROM ' || quote_ident( _db_name ) || '.sys.column                    AS c
         LEFT OUTER JOIN 
            ' || quote_ident( _db_name ) || '.pg_catalog.pg_column_encryption AS e   
              ON (( c.table_id = e.cerelid) AND ( c.column_id = e.cenum))
         WHERE
            table_id = ' || _rel_rec.table_id || '        
         ORDER BY column_id
         ';

         --RAISE INFO '_cols_sql = %', _cols_sql ;
         RETURN QUERY  EXECUTE ( _cols_sql );
         
         RETURN QUERY  EXECUTE $$SELECT (')')::VARCHAR(64000)$$;
         
         /* ****************************************************************************
         ** Table attributes
         */
         
         RETURN QUERY  EXECUTE  'SELECT ' || quote_literal( _rel_rec.distribution)     || '::VARCHAR(64000)';
         
         IF ( _rel_rec.sort_or_clstr_on IS NOT NULL ) THEN 
            RETURN QUERY  EXECUTE  'SELECT ' || quote_literal( _rel_rec.sort_or_clstr_on) || '::VARCHAR(64000)';   
         END IF;
         
         IF ( _rel_rec.prnt_on IS NOT NULL ) THEN 
            RETURN QUERY  EXECUTE  'SELECT ' || quote_literal( _rel_rec.prnt_on) || '::VARCHAR(64000)';   
         END IF;
    
         -- ending semicolon
         RETURN QUERY  EXECUTE  'SELECT ' || quote_literal( ';')     || '::VARCHAR(64000)';
         
/*       -- *************************************************************************   
         -- Constraints if not minimal mode.
         --
         SELECT 
              '   , ' || pg_get_constraintdef(k.oid) -- AS ddl       
            , 2                                      -- AS part      
            , k.conrelid                             -- AS table_oid 
            , k.oid::INT                             -- AS col_num   
         FROM
            pg_constraint k 
            JOIN
               usr_tbls t ON k.conrelid = t.table_oid
         WHERE
            k.contype <> 'c'   
            
         -- *************************************************************************   
         -- Set Owner if not minimal mode.    
                  -- *************************************************************************   
         -- Set permissions if not minimal mode.   
      
*/ 
         
      END LOOP;

   END LOOP;

END; 
$proc$
;


COMMENT ON FUNCTION rel_ddl_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
Generates a ddl for user table(s) as sequetial varchar rows.

Examples:
  SELECT * FROM rel_ddl_p( );
  SELECT * FROM rel_ddl_p( ''my_db'', ''s%'');
  SELECT * FROM rel_ddl_p( ''%'', ''%qtr%'' ,''%fact%'');  
  
Arguments:
. _db_ilike     - (optl) An ILIKE pattern for the schema name. i.e. ''%fin%''.
                  The default is ''%''
. _schema_ilike - (optl) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                  The default is ''%''
. _table_ilike  - (optl) An ILIKE pattern for the table name.  i.e. ''fact%''.
                  The default is ''%''

Version:
. 2022.12.02 - Yellowbrick Technical Support 
$cmnt$
;

