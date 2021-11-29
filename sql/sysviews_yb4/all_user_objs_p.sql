/* ****************************************************************************
** all_user_objs_p()
**
** All user objects in all databases with owner and ACL detail.
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
** . 2021.01.20 - fix: reset tags on exit.
** . 2020.10.30 - Yellowbrick Technical Support
** . 2020.11.18 - Integrated with YbEasyCli
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | obj_type |  schema_name  |  obj_name   | owner_name | owner_type |                acls
** -------------+----------+---------------+-------------+------------+------------+-------------------------------------
**  yellowbrick | SCHEMA   | public        | public      | ybdadmin   | USER       | {ybdadmin=UC/ybdadmin,=UC/ybdadmin}
**  yellowbrick | SCHEMA   | load_test     | load_test   | ybd_test   | USER       |
**  yellowbrick | TABLE    | public        | a11         | ybd_test   | USER       |
**  yellowbrick | TABLE    | public        | a12         | ybd_test   | USER       |
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/

DROP TABLE IF EXISTS all_user_objs_t CASCADE;
CREATE TABLE         all_user_objs_t
(
   db_name     VARCHAR (128)
 , obj_type    VARCHAR (16)
 , schema_name VARCHAR (128)
 , obj_name    VARCHAR (128)
 , owner_name  VARCHAR (128)
 , owner_type  VARCHAR (16)
 , acls        VARCHAR (60000)
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE all_user_objs_p(
   _yb_util_filter VARCHAR DEFAULT 'TRUE')
   RETURNS SETOF all_user_objs_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY INVOKER AS
$proc$
DECLARE

   _curr_db_sql         TEXT;
   _db_id               BIGINT;
   _db_list_sql         TEXT;
   _db_name             VARCHAR(128);
   _ret_sql             TEXT;   
   _sql                 TEXT;
   
   _db_rec              RECORD;
   _fn_name             VARCHAR(256) := 'all_user_objs_p';
   _prev_tags           VARCHAR(256) := current_setting('ybd_query_tags');
   _tags                VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END 
                                        || 'sysviews:' || _fn_name;       

BEGIN
 
   --SET TRANSACTION       READ ONLY;

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* Query for the databases to iterate over
   */
   _db_list_sql = 'SELECT database_id AS db_id, name AS db_name FROM sys.database ORDER BY name' ;
      
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
          , role_id    AS owner_id
          , name       AS owner_name
         FROM  ' || quote_ident(_db_name) || '.sys.role
      )
      , rels AS
      (  SELECT oid    AS obj_id
       , relnamespace AS schema_id
       , CASE relkind
            WHEN ''r'' THEN ''TABLE''::VARCHAR( 16 )
            WHEN ''v'' THEN ''VIEW''::VARCHAR( 16 )
            WHEN ''s'' THEN ''SEQUENCE''::VARCHAR( 16 )
         END      AS obj_type
       , relname  AS obj_name
       , relowner AS owner_id
       , relacl   AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_class
      WHERE oid > 16384
         AND( relnamespace  = 2200
            OR relnamespace > 16384 )
         AND relkind IN( ''r'', ''v'', ''s'' )
         AND relname NOT LIKE ''yb_deletes_%''
      )
      , schemas AS
      ( SELECT oid               AS obj_id
       , oid                     AS schema_id
       , ''SCHEMA''::VARCHAR( 16 ) AS obj_type
       , nspname                 AS obj_name
       , nspowner                AS owner_id
       , nspacl                  AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace
      WHERE( oid   = 2200
            OR oid > 16384 )
         AND nspname NOT LIKE ''pg_t%''
      )
      , procs AS
      ( SELECT p.oid    AS obj_id
       , p.pronamespace AS schema_id
       , CASE l.lanname
            WHEN ''plpgsql'' THEN ''PROCEDURE''::VARCHAR( 16 )
            WHEN ''ybcpp'' THEN ''UDF'':: VARCHAR( 16 )
         END              AS obj_type
       , proname          AS obj_name
       , proowner         AS owner_id
       , proacl           AS acl
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_proc           p
         JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_language    l ON p.prolang = l.oid
      WHERE p.oid                           > 16384
         AND( p.pronamespace  = 2200
            OR p.pronamespace > 16384 )
         AND l.lanname IN( ''plpgsql'', ''ybcpp'' )
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
      FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_default_acl          da
         LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace    n ON da.defaclnamespace = n.oid
      )
      , objs AS
      ( 
                     SELECT *   FROM rels
         UNION ALL   SELECT *   FROM procs
         UNION ALL   SELECT *   FROM schemas
         UNION ALL   SELECT *   FROM dflt_acls   
      )
      , objattrs AS
      ( SELECT 
         '|| quote_literal(_db_name) || ' AS db_name
       , r.obj_id                 AS obj_id
       , r.obj_type               AS obj_type
       , r.schema_id              AS schema_id
       , s.nspname                AS schema_name
       , r.obj_name               AS obj_name
       , CASE
            WHEN obj_type != ''SCHEMA'' AND obj_type NOT ilike ''DFLT%'' THEN s.nspname
                  || ''.''
            ELSE ''''
         END
            || r.obj_name       AS fqn
       , r.owner_id             AS owner_id
       , o.owner_type           AS owner_type
       , o.owner_name           AS owner_name
       , r.acl                  AS acl
      FROM objs                    r
         LEFT JOIN ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace    s ON r.schema_id = s.oid
         LEFT JOIN owners          o ON r.owner_id  = o.owner_id
      )
      SELECT 
        db_name::VARCHAR(128)
      , obj_type::VARCHAR(16)
      , schema_name::VARCHAR(128)
      , obj_name::VARCHAR(128)
      , owner_name::VARCHAR(128)
      , owner_type::VARCHAR(16)
      , acl::VARCHAR(60000)
      FROM objattrs
      WHERE ' || _yb_util_filter || '
      ORDER BY obj_type, schema_id, obj_name 
      '
      ;
 
      RETURN QUERY EXECUTE _curr_db_sql;
         
   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );         

END;
$proc$
;

COMMENT ON FUNCTION all_user_objs_p(VARCHAR) IS 
'Description:
All user objects in all databases with owner and ACL detail. 

Includes database, schema, owner, owner type, and ACLs for all schemas, tables,  
views, sequences, stored procedures, UDFs, and default ACLs.
  
Examples:
  SELECT * FROM all_user_objs_p() 
  SELECT * FROM all_user_objs_p() WHERE obj_type = ''SCHEMA''  
  
Arguments:
. None.

Version:
. 2021.01.20 - Yellowbrick Technical Support
. 2020.11.18 - Integrated with YbEasyCli
'
;