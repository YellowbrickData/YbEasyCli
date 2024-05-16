/* ****************************************************************************
** all_obj_grants_p()
**
** Grants on user objects across all databases with owner detail.
** Includes: databases, schemas, tables, views, sequences, stored procedures
**          ,columns, roles, and keys.
** Excludes: system tables/views and triggers.
**           superuser implicit grants
**           owner implicit grants
**           nested role membership
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** TODO:
** . Need speical handling for DFLTACL?
**
** ABOUT GRANTED PRIVILEGES
** . Granted priveleges are stored in pg tables *.relacl as an array of acl items. 
** . There may be 0 or more ACLs in the ACL array. i.e.:
**   {dba_role=arwdRxt/yellowbrick,"ro_role=r/yellowbrick","rw_role=arwd/yellowbrick"} 
** . aclitems array format:
**      grantee=privs/grantor 
**   WHERE -- Role granted to privileges granted to a role are one or more of the following
** 
** Key SYSTEM               DATABASE        SCHEMAS    DEFAULTACLs   TABLES/VIEWS   COLUMNS    SEQUENCES &PROCS  ROLES      KEYS
** --- -------------------  --------------- ---------  ------------  ------------   ---------- --------- ------- ---------- --------
** a                        BULK LOAD                  INSERT        INSERT         INSERT                                 
** r   VIEW ROLE                                                     SELECT         SELECT     SELECT                               
** w                        ALTER ANY SCHEMA           UPDATE        UPDATE         UPDATE     UPDATE                          
** d                        DROP ANY SCHEMA            DELETE        DELETE         DELETE                               
** D                                                   TRUNCATE      TRUNCATE                                              
** x                                                   REFERENCES    REFERENCES     REFERENCES                               
** t                                                                                                                       
** X   CONTROL LDAP                                                                                       EXECUTE                  
** U   CONTROL ANY SESSION                  USAGE                                              USAGE                          
** C   CREATE ROLE          CREATE          CREATE                                                                         
** T                        CREATE TEMP                                                                                    
** c                        CONNECT                                                                                        
** e                                                                                                                         ENCRYPT
** E                                                                                                                         DECRYPT
** h                                                                                                                         HMAC_KS
** b   CREATE DATABASE                                                                                                     
** p   EXPLAIN QUERY        EXPLAIN QUERY                                                                                  
** q   VIEW QUERY TEXT      VIEW QUERY TEXT                                                                                
** Q   TRACE QUERY          TRACE QUERY                                                                                    
** A   ALTER ANY ROLE                                                                                            ALTER ROLE        
** B   DROP ANY ROLE                                                                                             DROP ROLE        
** u   BACKUP ANY DATABASE  BACKUP                                                                                       
** O   RESTORE ANY DATABASE RESTORE                                                                                       
** Z                        CONTROL                                                                              CONTROL        
**
** See also: https://docs.yellowbrick.com/5.2.27/administration/abbrevs_access_privileges.html#abbreviations-for-acls
** 
** Example:
**    arwdxt -- ALL PRIVILEGES (for tables)
**    *      -- grant option for preceding privilege
**
** datacl | {=Tc/kick,kick=awdCTcpqQuOZ/kick,sys_ybd_acl_grantor=a/kick}
**
** (c) 2018-2023 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** 2023.12.21 - Fix multiple grant permission matches.
**              Added missing 'b' ACL key.
**              Formatting and comment changes.
** 2023.12.19 - Rename to all_obj_grants_p
**              Fix minor perms issues for DATABASE and SCHEMA.
** 2023.12.13 - Fix for DFLTACL -> UNKNOWN. (Yellowbrick TS) 
** 2023.11.06 - Initial draft version. (Yellowbrick TS) 
*/

/* ****************************************************************************
**  Example results:
**
**  db_name | obj_type  | schema_name |           obj_name            | owner_name  |    user_or_role     |       grantee       | grantor  |   granted    |     a      |   r    |        w         |        d        |    D     |     x     | X |   U   |C       |        T         |    c    | e | E | h | b |       p       |        q        |      Q      | A | B |   u    |    O    |    Z
** ---------+-----------+-------------+-------------------------------+-------------+---------------------+---------------------+----------+--------------+------------+--------+------------------+-----------------+----------+-----------+---+-------+--------+------------------+---------+---+---+---+---+---------------+-----------------+-------------+---+---+--------+---------+---------
**  kick    | DATABASE  |             | kick                          | kick        | public              | public              | kick     | aTc          | BULK LOAD  |        |                  |                 |          |           |   |       |        | CREATE TEMPORARY | CONNECT |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | DATABASE  |             | kick                          | kick        | kick                | kick                | kick     | awdCTcpqQuOZ | BULK LOAD  |        | ALTER ANY SCHEMA | DROP ANY SCHEMA |          |           |   |       | CREATE | CREATE TEMPORARY | CONNECT |   |   |   |   | EXPLAIN QUERY | VIEW QUERY TEXT | TRACE QUERY |   |   | BACKUP | RESTORE | CONTROL
**  kick    | DATABASE  |             | kick                          | kick        | sys_ybd_acl_grantor | sys_ybd_acl_grantor | kick     | a*           | BULK LOAD+ |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | DFLTACL   | public      | "public" Schema Rel  dflt acl | r_kick      | public              | public              | r_kick   | r            |            | SELECT |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | DFLTACL   | public      | "public" Schema Rel  dflt acl | r_kick      | dbadmin_r           | dbadmin_r           | r_kick   | arwdDxt      | INSERT     | SELECT | UPDATE           | DELETE          | TRUNCATE | REFERENCE |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | PROCEDURE | public      | all_obj_grants_p              | yellowbrick |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | PROCEDURE | public      | all_user_obj_perms_p          | yellowbrick |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | PROCEDURE | public      | cat_sf_insert_p               | kick        |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | PROCEDURE | public      | test_sbtxn_p                  | yellowbrick |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | SCHEMA    | public      | public                        | ybdadmin    | ybdadmin            | ybdadmin            | ybdadmin | UC           |            |        |                  |                 |          |           |   | USAGE | CREATE |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | SCHEMA    | public      | public                        | ybdadmin    | public              | public              | ybdadmin | UC           |            |        |                  |                 |          |           |   | USAGE | CREATE |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | SCHEMA    | schema2     | schema2                       | yellowbrick |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | TABLE     | public      | Foo                           | kick        |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
**  kick    | TABLE     | public      | a                             | kick        |                     |                     |          |              |            |        |                  |                 |          |           |   |       |        |                  |         |   |   |   |   |               |                 |             |   |   |        |         |
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/

DROP TABLE IF EXISTS all_obj_grants_t CASCADE;

CREATE TABLE         all_obj_grants_t
(
   db_name      VARCHAR (128)
 , obj_type     VARCHAR (16)
 , schema_name  VARCHAR (128)
 , obj_name     VARCHAR (128)
 , owner_name   VARCHAR (128) 
 , user_or_role VARCHAR (128)
 , grantee      VARCHAR (128)
 , grantor      VARCHAR (128) 
 , granted      VARCHAR (1024) 
 , "a"          VARCHAR (32) 
 , "r"          VARCHAR (32) 
 , "w"          VARCHAR (32) 
 , "d"          VARCHAR (32) 
 , "D"          VARCHAR (32) 
 , "x"          VARCHAR (32) 
 , "X"          VARCHAR (32)
 , "U"          VARCHAR (32)
 , "C"          VARCHAR (32)
 , "T"          VARCHAR (32)
 , "c"          VARCHAR (32)
 , "e"          VARCHAR (32)
 , "E"          VARCHAR (32)
 , "h"          VARCHAR (32) 
 , "b"          VARCHAR (32) 
 , "p"          VARCHAR (32)
 , "q"          VARCHAR (32)
 , "Q"          VARCHAR (32)
 , "A"          VARCHAR (32)
 , "B"          VARCHAR (32)
 , "u"          VARCHAR (32)
 , "O"          VARCHAR (32)
 , "Z"          VARCHAR (32)
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE all_obj_grants_p(
     _db_ilike       VARCHAR DEFAULT '%'
   , _schema_ilike   VARCHAR DEFAULT '%'
   , _obj_ilike      VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF all_obj_grants_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY INVOKER 
AS
$proc$
DECLARE

   _curr_db_sql         TEXT;
   _db_id               BIGINT;
   _db_list_sql         TEXT;
   _db_name             VARCHAR(128);
   _pred                TEXT;   
   _ret_sql             TEXT;
   _sql                 TEXT;
   
   _db_rec              RECORD;
   _fn_name             VARCHAR(256) := 'all_obj_grants_p';
   _prev_tags           VARCHAR(256) := current_setting('ybd_query_tags');
   _tags                VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END 
                                        || 'sysviews:' || _fn_name;       
   

BEGIN
 
   --EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags );
   --PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);
   
   -- TODO update this so nulls are accounted for if ilike %
   _pred := '      db_name    ILIKE ' || quote_literal( _db_ilike     ) || '
            AND (schema_name  ILIKE ' || quote_literal( _schema_ilike ) || ' OR schema_name IS NULL)
            AND (    obj_name ILIKE ' || quote_literal( _obj_ilike    ) || '
                  OR obj_name ILIKE ' || '''' || _obj_ilike || '.%'''   || '
                )
   ';
      
   -- Query for the databases to iterate over
   _db_list_sql = 'SELECT 
      database_id AS db_id, name AS db_name 
   FROM sys.database 
   WHERE db_name ILIKE ' || quote_literal( _db_ilike ) || ' ORDER BY name' ;
   -- RAISE info '_db_list_sql = %', _db_list_sql;

   /* Iterate over each db and get the relation metadata including schema 
   */
   FOR _db_rec IN EXECUTE _db_list_sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      --RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ;

      _curr_db_sql := 'WITH owners AS
      (  SELECT ''USER'' AS owner_type
          , user_id      AS owner_id
          , name         AS owner_name
         FROM ' || quote_ident(_db_name) || '.sys.user
         UNION ALL
         SELECT ''ROLE'' AS owner_type
          , role_id      AS owner_id
          , name         AS owner_name
         FROM  ' || quote_ident(_db_name) || '.sys.role
         LIMIT 100000
      )
      
      , rels AS
      (  SELECT oid    AS obj_id
       , relnamespace  AS schema_id
       , CASE relkind
            WHEN ''r'' THEN ''TABLE''::VARCHAR( 16 )
            WHEN ''v'' THEN ''VIEW''::VARCHAR( 16 )
            WHEN ''s'' THEN ''SEQUENCE''::VARCHAR( 16 )
         END      AS obj_type
       , relname  AS obj_name
       , relowner AS owner_id
       , relacl   AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_class
      WHERE  oid     > 16384
         AND relkind IN( ''r'', ''v'', ''s'' )
         AND relname NOT LIKE ''yb_deletes_%''
      LIMIT 100000
      )
      
      , cols AS
      ( SELECT r.obj_id                                AS obj_id
       , r.schema_id                                   AS schema_id
       , r.obj_type || ''.COLUMN''::VARCHAR( 16 )      AS obj_type
       , r.obj_name || ''.'' || a.attname              AS obj_name
       , r.owner_id                                    AS owner_id
       , a.attacl                                      AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_attribute AS a
      JOIN rels                                                     AS r ON a.attrelid = r.obj_id
      WHERE a.attacl IS NOT NULL
      )
      
      , schemas AS
      ( SELECT oid                 AS obj_id
       , oid                       AS schema_id
       , ''SCHEMA''::VARCHAR( 16 ) AS obj_type
       , nspname                   AS obj_name
       , nspowner                  AS owner_id
       , nspacl                    AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace
      WHERE(   oid = 2200
            OR oid > 16384 
           )
         AND nspname NOT LIKE ''pg_t%''
      )
      
      , procs AS
      ( SELECT p.oid    AS obj_id
       , p.pronamespace AS schema_id
       , CASE l.lanname
            WHEN ''plpgsql'' THEN ''PROCEDURE''::VARCHAR( 16 )
            WHEN ''ybcpp''   THEN ''UDF'':: VARCHAR( 16 )
         END              AS obj_type
       , proname          AS obj_name
       , proowner         AS owner_id
       , proacl           AS acl
      FROM ' || quote_ident(_db_name) || '.pg_catalog.pg_proc     AS p
      JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_language AS l ON p.prolang = l.oid
      WHERE p.oid > 16384 
         AND l.lanname IN( ''plpgsql'', ''ybcpp'', ''sql'' )
      )
      
      , dflt_acls AS
      ( SELECT da.oid       AS obj_id
       , da.defaclnamespace AS schema_id
       , CASE da.defaclobjtype
            WHEN ''r'' THEN ''DFLTACL''::VARCHAR( 16 )
            WHEN ''f'' THEN ''DFLTACL''::VARCHAR( 16 )
            WHEN ''S'' THEN ''DFLTACL''::VARCHAR( 16 )
         END AS obj_type
       , CASE da.defaclobjtype
            WHEN ''r'' THEN NVL( ''"''||n.nspname || ''"'', ''Default'' ) || '' Schema Rel  dflt acl''
            WHEN ''f'' THEN NVL( ''"''||n.nspname || ''"'', ''Default'' ) || '' Schema Proc dflt acl''
            WHEN ''S'' THEN NVL( ''"''||n.nspname || ''"'', ''Default'' ) || '' Schema Seq  dflt acl''
         END                    AS obj_name
       , da.defaclrole          AS owner_id
       , da.defaclacl           AS acl
      FROM      ' || quote_ident(_db_name) || '.pg_catalog.pg_default_acl AS da
      LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace   AS n  ON da.defaclnamespace = n.oid
      )
      
      , encrypt_keys AS
      ( SELECT k.oid                  AS obj_id
       , k.keynamespace               AS schema_id
       , ''KEY''::VARCHAR( 16 )       AS obj_type
       , k.keyname                    AS obj_name
       , k.keyowner                   AS owner_id
       , k.keyacl                     AS acl
      FROM      ' || quote_ident(_db_name) || '.pg_catalog.pg_keystore  AS k       
      LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace AS n ON k.keynamespace = n.oid
      WHERE k.keydatabase = ' || _db_id || '
      )   
      
      , dbs AS
      ( SELECT oid                    AS obj_id
       , NULL::INT4                   AS schema_id
       , ''DATABASE''::VARCHAR( 16 )  AS obj_type
       , datname                      AS obj_name
       , datdba                       AS owner_id
       , datacl                       AS acl
       FROM pg_database 
       WHERE datname = ' || quote_literal(_db_name) || '
      )   
      
      , objs AS
      ( 
                     SELECT *   FROM dbs
         UNION ALL   SELECT *   FROM rels   
         UNION ALL   SELECT *   FROM cols
         UNION ALL   SELECT *   FROM procs
         UNION ALL   SELECT *   FROM schemas
         UNION ALL   SELECT *   FROM dflt_acls  
      )
      
      , obj_attrs AS
      ( SELECT 
         '|| quote_literal(_db_name) || ' AS db_name
       , o.obj_id                 AS obj_id
       , o.obj_type               AS obj_type
       , o.schema_id              AS schema_id
       , n.nspname                AS schema_name
       , o.obj_name               AS obj_name
       , CASE
            WHEN obj_type NOT IN ( ''DATABASE'', ''SCHEMA'', ''DFLTACL'' ) THEN n.nspname || ''.''
            ELSE ''''
         END
         || o.obj_name          AS fqn
       , o.owner_id             AS owner_id
       , u.owner_type           AS owner_type
       , u.owner_name           AS owner_name
       , o.acl                  AS acl
      FROM objs                                                            AS o
         LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace AS n ON o.schema_id = n.oid
         LEFT JOIN owners                                                  AS u ON o.owner_id  = u.owner_id
      )
      ,obj_acls AS
      (
         SELECT 
           db_name::VARCHAR(128)          AS db_name
         , obj_type::VARCHAR(16)          AS obj_type
         , schema_name::VARCHAR(128)      AS schema_name
         , obj_name::VARCHAR(128)         AS obj_name
         , owner_name::VARCHAR(128)       AS owner_name
         , UNNEST( acl )::VARCHAR( 1024 ) AS acl     
         FROM obj_attrs
         WHERE acl IS NOT NULL   
           AND ' || _pred           || '
           AND ' || _yb_util_filter || '    

         UNION ALL 
         SELECT 
           db_name::VARCHAR(128)          AS db_name
         , obj_type::VARCHAR(16)          AS obj_type
         , schema_name::VARCHAR(128)      AS schema_name
         , obj_name::VARCHAR(128)         AS obj_name
         , owner_name::VARCHAR(128)       AS owner_name
         , NULL::VARCHAR( 1024 )          AS acl     
  
         FROM obj_attrs
         WHERE acl IS NULL
           AND ' || _pred           || '
           AND ' || _yb_util_filter || '  

      )
      , obj_grants AS
      (
         SELECT 
           db_name::VARCHAR(128)                                                                   AS db_name
         , obj_type::VARCHAR(16)                                                                   AS obj_type
         , schema_name::VARCHAR(128)                                                               AS schema_name
         , obj_name::VARCHAR(128)                                                                  AS obj_name
         , owner_name::VARCHAR (128)                                                               AS owner_name 
         , IIF(SUBSTR(acl,1,1) = ''='', ''public'', (SPLIT_PART(acl, ''='', 1)))::VARCHAR (128)    AS user_or_role
         , IIF(SUBSTR(acl,1,1) = ''='', ''public'', (SPLIT_PART(acl, ''='', 1)))::VARCHAR (128)    AS grantee   
         , SPLIT_PART(acl, ''/'', 2)::VARCHAR (128)                                                AS grantor     
         , SPLIT_PART(SPLIT_PART(acl, ''/'', 1), ''='', 2)::VARCHAR (1024)                         AS granted   
      /* , IIF(STRPOS(NVL(granted,''''), ''*'') = 0, ''F'', ''T'')::BOOLEAN                        AS with_grant */
         
         , (IIF(STRPOS(NVL(granted,''''), ''a'') = 0,  NULL, DECODE( obj_type,
              ''DATABASE'',''BULK LOAD'',  ''DFLTACL'',''INSERT'',  ''TABLE'',''INSERT'',  ''VIEW'',''INSERT'' 
              ,''TABLE.COLUMN'',''INSERT'',  ''VIEW.COLUMN'',''INSERT'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''a*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "a"
          
         , (IIF(STRPOS(NVL(granted,''''), ''r'') = 0,  NULL, DECODE( obj_type, 
             ''SYSTEM'',''VIEW ROLE'',  ''DFLTACL'',''SELECT'',  ''TABLE'',''SELECT'' 
            ,''VIEW'',''SELECT'',  ''TABLE.COLUMN'',''SELECT'',  ''VIEW.COLUMN'',''SELECT'' 
            ,''SEQUENCE'',''SELECT'',  ''UNKNOWN''
            ))
          ||IIF(STRPOS(NVL(granted,''''), ''r*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "r"
          
         , (IIF(STRPOS(NVL(granted,''''), ''w'') = 0,  NULL, DECODE( obj_type, 
             ''DATABASE'', ''ALTER ANY SCHEMA'',  ''DFLTACL'',''UPDATE'',  ''TABLE'',''UPDATE''  
            ,''VIEW'',''UPDATE'',  ''TABLE.COLUMN'',''UPDATE'', ''VIEW.COLUMN'',''UPDATE''
            , ''SEQUENCE'', ''UPDATE'',  ''UNKNOWN''
            ))
          ||IIF(STRPOS(NVL(granted,''''), ''w*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "w"
          
         , (IIF(STRPOS(NVL(granted,''''), ''d'') = 0,  NULL, DECODE( obj_type, 
             ''DATABASE'',''DROP ANY SCHEMA'',  ''DFLTACL'',''DELETE''
            ,''TABLE'',''DELETE'',  ''VIEW'',''DELETE'',  ''UNKNOWN''
            ))
          ||IIF(STRPOS(NVL(granted,''''), ''r*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "d"
           
         , (IIF(STRPOS(NVL(granted,''''), ''D'') = 0,  NULL, DECODE( obj_type, 
            ''DFLTACL'',''TRUNCATE'',  ''TABLE'',''TRUNCATE'',  ''VIEW'',''TRUNCATE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''D*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "D"
           
         , (IIF(STRPOS(NVL(granted,''''), ''x'') = 0,  NULL, DECODE( obj_type, 
            ''DFLTACL'',''REFERENCE'',  ''TABLE'',''REFERENCE'',  ''VIEW'',''REFERENCE''
            ,''TABLE.COLUMN'',''REFERENCES'',  ''VIEW.COLUMN'',''REFERENCES'',  ''UNKNOWN''
           ))
         ||IIF(STRPOS(NVL(granted,''''), ''x*'') = 0,  '''', ''+''))::VARCHAR(32)                  AS "x"
           
         , (IIF(STRPOS(NVL(granted,''''), ''X'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''CONTROL LDAP'',  ''PROCEDURE'',''EXECUTE'',  ''UDF'',''EXECUTE'',  ''UNKNOWN''
           ))
           ||IIF(STRPOS(NVL(granted,''''), ''X*'') = 0,  '''', ''+''))::VARCHAR(32)                AS "X"      
           
         , (IIF(STRPOS(NVL(granted,''''), ''U'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''CONTROL ANY SESSION'',  ''SCHEMA'',''USAGE'',  ''SEQUENCE'',''USAGE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''U*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "U"  
          
         , (IIF(STRPOS(NVL(granted,''''), ''C'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''CREATE ROLE'',  ''DATABASE'',''CREATE'',  ''SCHEMA'',''CREATE'',  ''UNKNOWN''
           ))
           ||IIF(STRPOS(NVL(granted,''''), ''C*'') = 0,  '''', ''+''))::VARCHAR(32)                AS "C"  
           
         /* t (trigger) is ignored                                                                     */
         
         , (IIF(STRPOS(NVL(granted,''''), ''T'') = 0,  NULL, DECODE( obj_type, 
            ''DATABASE'',''CREATE TEMPORARY'',  ''UNKNOWN''
           ))
         ||IIF(STRPOS(NVL(granted,''''), ''T*'') = 0,  '''', ''+''))::VARCHAR(32)                  AS "T"  
           
         , (IIF(STRPOS(NVL(granted,''''), ''c'') = 0,  NULL, DECODE( obj_type, 
            ''DATABASE'',''CONNECT'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''c*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "c"       
           
         , (IIF(STRPOS(NVL(granted,''''), ''e'') = 0,  NULL, DECODE( obj_type, 
            ''KEY'',''ENCRYPT'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''e*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "e" 
          
         , (IIF(STRPOS(NVL(granted,''''), ''E'') = 0,  NULL, DECODE( obj_type, 
            ''KEY'',''DECRYPT'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''E*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "E" 
           
         , (IIF(STRPOS(NVL(granted,''''), ''h'') = 0,  NULL, DECODE( obj_type, 
            ''KEY'',''HMAC_KS'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''h*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "h" 

         , (IIF(STRPOS(NVL(granted,''''), ''b'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''CREATE DATABASE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''b*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "b"
          
         , (IIF(STRPOS(NVL(granted,''''), ''p'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''EXPLAIN QUERY'',  ''DATABASE'',''EXPLAIN QUERY'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''p*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "p"
          
         , (IIF(STRPOS(NVL(granted,''''), ''q'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''VIEW QUERY TEXT'',  ''DATABASE'',''VIEW QUERY TEXT'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''q*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "q"
           
         , (IIF(STRPOS(NVL(granted,''''), ''Q'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''TRACE QUERY'',  ''DATABASE'',''TRACE QUERY'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''Q*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "Q"
          
         , (IIF(STRPOS(NVL(granted,''''), ''A'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''ALTER ANY ROLE'',  ''USER'',''ALTER ROLE'',  ''ROLE'',''ALTER ROLE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''A*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "A"
          
         , (IIF(STRPOS(NVL(granted,''''), ''B'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''DROP ANY ROLE'',  ''USER'',''DROP ROLE'',  ''ROLE'',''DROP ROLE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''B*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "B"
          
         , (IIF(STRPOS(NVL(granted,''''), ''u'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''BACKUP ANY DATABASE'',  ''DATABASE'',''BACKUP'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''u*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "u"
          
         , (IIF(STRPOS(NVL(granted,''''), ''O'') = 0,  NULL, DECODE( obj_type, 
            ''SYSTEM'',''RESTORE ANY DATABASE'',  ''DATABASE'',''RESTORE'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''O*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "O"
          
         , (IIF(STRPOS(NVL(granted,''''), ''Z'') = 0,  NULL, DECODE( obj_type, 
            ''DATABASE'',''CONTROL'',  ''USER'',''CONTROL'',  ''ROLE'',''CONTROL'',  ''UNKNOWN''
           ))
          ||IIF(STRPOS(NVL(granted,''''), ''D*'') = 0,  '''', ''+''))::VARCHAR(32)                 AS "Z"

         FROM obj_acls     
      )
      SELECT * 
      FROM obj_grants
      ORDER BY obj_type, schema_name, obj_name 
      '
      ;

      --RAISE INFO '_curr_db_sql=%', _curr_db_sql;
      RETURN QUERY EXECUTE _curr_db_sql;
      

   END LOOP;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );

END;
$proc$
;

COMMENT ON FUNCTION all_obj_grants_p(VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
$cmnt$Description:
All user objects in all databases with owner and ACL detail. 

Includes database, schema, owner, and ACLs for all databases, schemas, tables,  
views, sequences, stored procedures, UDFs, default ACLs, encryption keys, 
explicit granted table and view columns, and global system grants.
  
Examples:
  SELECT * FROM all_obj_grants_p() 
  SELECT * FROM all_obj_grants_p('%prod%', 's%') WHERE obj_type = 'SCHEMA'  
  
Arguments:
. _db_ilike       VARCHAR (optl)  - An ILIKE pattern for the database name. Default is '%'.
. _schema_ilike   VARCHAR (optl)  - An ILIKE pattern for the schema   name. Default is '%'.
. _obj_ilike      VARCHAR (optl)  - An ILIKE pattern for the object   name. Default is '%'.
. _yb_util_filter VARCHAR (intrn) - Used by YbEasyCli.

Note:
. Objects not belonging to a schema (like databases and default ACLs) are always 
  returned even if a schema filter is applied. 
. "WITH GRANT" is displayed with a "+" character following the permission.
. There is no exposed "SYSTEM" object with ACLs. Use has_system_function().

Version:
. 2023.12.21 - Yellowbrick Technical Support
$cmnt$
;

\timing on
-- -----------------------------------------------------------------------------
SELECT * FROM all_obj_grants_p('kick'                  ) LIMIT 40;
\q
SELECT * FROM all_obj_grants_p('kick', 'public', 'foo%');
SELECT * FROM all_obj_grants_p('kick', 'public', 'c%'  );
SELECT * FROM all_obj_grants_p('%'                     ) WHERE obj_type = 'DFLTACL' LIMIT 40;
SELECT * FROM all_obj_grants_p('aci%');
SELECT '~~~~~~~~~~~~~~~~~~~~~~~~~~~~DONE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'