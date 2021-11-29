/* ****************************************************************************
** _view_ddls_p() 
** 
** Generates ddls for user views(s) (as sequential varchar rows) including owner
** and GRANTs for a given database.
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
** . 2020.11.10 - Yellowbrick Technical Support 
*/
 

DROP PROCEDURE IF EXISTS    view_ddls_p( VARCHAR, VARCHAR, VARCHAR ) CASCADE;
CREATE OR REPLACE PROCEDURE view_ddls_p( _db_name VARCHAR DEFAULT current_database(), _schema_ilike VARCHAR DEFAULT '%', _view_ilike VARCHAR DEFAULT '%' )
RETURNS SETOF sys.vt_worker_states
AS
$$
DECLARE

   _pred      TEXT;
   _sql       TEXT;
   /* This is a trick to be able to have a return type column of TEXT 
   ** sys.vt_worker_states has 2 columns, both TEXT: id, and state
   */
   _rec       RECORD;
   _ret_rec   sys.vt_worker_states%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'view_ddls_t';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   /* Txn read_only to help protect against potential SQL injection attack writes
   */
   SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _pred := 'WHERE  '
   || '     database_name =     ' || quote_literal( _db_name ) 
   || ' AND schema_name   ILIKE ' || quote_literal( _schema_ilike )
   || ' AND view_name     ILIKE ' || quote_literal( _view_ilike  ) 
   || ' ' || CHR(10);
   
   _sql :='
   WITH owners AS
   ( SELECT user_id AS owner_id
    , name          AS owner_name
   FROM sys.user
   UNION ALL
   SELECT role_id AS owner_id
    , name        AS owner_name
   FROM sys.role
   )
   , usr_views AS
   ( SELECT 
      v.database_id
    , v.database_name              AS database_name
    , v.view_id                    AS view_id
    , v.schema_id                  AS schema_id
    , s.name                       AS schema_name
    , v.name                       AS view_name
    , v.owner_id                   AS owner_id
    , o.owner_name                 AS owner_name
    , OCTET_LENGTH( v.definition ) AS definition_len
    , v.definition                 AS definition
   FROM    sys.view   v
      JOIN sys.schema s ON v.schema_id = s.schema_id AND v.database_id = s.database_id
      JOIN owners     o ON v.owner_id  = o.owner_id
   ' || _pred || '
   )
   
   SELECT 
      * 
   FROM usr_views
   ORDER BY view_id ASC
   ';

   FOR _rec IN EXECUTE _sql 
   LOOP
      _ret_rec.id    := _rec.view_id;
      _ret_rec.state := E'-- --------------------------------------------------------------------------\n' 
            || E'-- db: ' || _rec.database_name || ', view_id: ' || _rec.view_id 
                 || ', view_name: ' || _rec.schema_name || '.' || _rec.view_name || E'\n'
            || E'-- --------------------------------------------------------------------------'
      ;
      --RAISE INFO '_rec.state=%s', _rec.state;
      RETURN NEXT _ret_rec ;
      
      _ret_rec.state := 'CREATE OR REPLACE VIEW ' || _rec.schema_name || '.' || _rec.view_name || E' AS\n'
            || _rec.definition || E'\n;\n';
      RETURN NEXT _ret_rec ;
      
      _ret_rec.state := 'ALTER VIEW IF EXISTS ' || _rec.schema_name || '.' || _rec.view_name || ' WITH OWNER ' || quote_ident( _rec.owner_name ) || E'\n;\n';
      RETURN NEXT _ret_rec ;
         
   END LOOP;
         
   /* Gen VIEW ACLs
   */
   _sql := 'WITH 
      acls AS
      (  SELECT
            c.oid                                AS view_id
          , n.nspname                            AS schema_name      
          , n.nspname || ''.'' || c.relname      AS fq_view_name
          , unnest( c.relacl )::varchar( 300 ) AS rel_privs
         FROM
            ' || quote_ident( _db_name ) || '.' || 'pg_catalog.pg_class               c
            LEFT JOIN 
            ' || quote_ident( _db_name ) || '.' || 'pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE
            c.oid IN( SELECT view_id FROM 
                      (  SELECT 
                           v.view_id       AS view_id
                         , v.database_name AS database_name
                         , s.name          AS schema_name
                         , v.name          AS view_name
                         FROM sys.view  v
                         JOIN sys.schema s ON v.schema_id = s.schema_id AND v.database_id = s.database_id
                         ' || _pred || '
                      ) sq
                    )
      )
    , acl_parts AS
      (  SELECT
            view_id
          , fq_view_name
          , CASE
               WHEN LEFT( rel_privs, 1 ) = ''='' THEN ''default''
               ELSE ''role''
            END AS type
          , rel_privs::varchar( 256 )
          , split_part( rel_privs, ''='', 1 )                       AS role
          , split_part( split_part( rel_privs, ''='', 2 ), ''/'', 1 ) AS perms
          , split_part( rel_privs, ''/'', 2 )                       AS rule
         FROM
            acls
      )
    , acl_grants AS
      (  SELECT  *
          , CASE
               WHEN perms = ''arwdDxt'' THEN ''ALL''
               ELSE REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( array_to_string( regexp_split_to_array( perms, '''' ), '', '' )
                            , ''a'', ''INSERT''     )
                            , ''r'', ''SELECT''     )
                            , ''D'', ''TRUNCATE''   )                         
                            , ''w'', ''UPDATE''     )
                            , ''d'', ''DELETE''     )
                            , ''x'', ''REFERENCES'' )
                            , ''t'', ''TRIGGER''    )
            
            END AS priv_names
         FROM
            acl_parts
      )

   SELECT
      view_id AS view_id
    , (''/* '' || rel_privs || '' */'' || E''\n'' ||
      CASE
         WHEN type = ''default'' THEN ''REVOKE ALL ON '' || fq_view_name || '' FROM PUBLIC ;'' || E''\n''
         ELSE ''''
      END || 
      ''GRANT '' || priv_names || '' ON '' || fq_view_name || '' TO '' ||
      CASE
         WHEN type = ''role'' THEN role
         ELSE ''PUBLIC''
      END || '' ;'' || E''\n''
      )      AS state
   FROM
      acl_grants
   ORDER BY
      fq_view_name, type, role, priv_names
   ';
   
   --RAISE INFO '_sql=%s', _sql;
   FOR _rec IN EXECUTE _sql 
   LOOP
      _ret_rec.id    := _rec.view_id;
      _ret_rec.state := _rec.state;
      
      RETURN NEXT _ret_rec ;
         
   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
;

COMMENT ON PROCEDURE view_ddls_p( VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
Generates DDLs for all user views(s) in a database including GRANT and ALTER OWNER 
as sequential varchar rows.

Examples:
  SELECT FROM view_ddls_p( );
  SELECT state FROM view_ddls_p( ''my_db'');
  SELECT * FROM view_ddls_p( ''my_db'', ''dev%'', ''%tmp%'' );  
  
Arguments:
. _db_name      - (optional) The database name. i.e. ''yellowbrick''.
. _schema_ilike - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                  The default is ''%''
. _view_ilike   - (optional) An ILIKE pattern for the view name.  i.e. ''fact%''.
                  The default is ''%''

Version:
. 2020.11.10 - Yellowbrick Technical Support 
';

