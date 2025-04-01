/* ****************************************************************************
** db_obj_grants_p()
** 
** Permissions on user objects in the current databases with owner detail.
** Includes: system, databases, schemas, tables, views, sequences, stored procedures
**          ,columns, roles, and keys.
** Excludes: system tables/views/functions, triggers, and default ACL objects.  
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** CAVEATS:
** . WARNING: if you run this for all users for all objects you can run your 
**   manager node out of disk catalog space as the massive join will spill to
**   temp space that is under the catalog directory.
**
** NOTES:
** . While has_table_id can be used with the form has_table_privilege( user_id, table_id, privilege)
**           it cannot be used in the context of a foreign database.
**
** ABOUT GRANTED PRIVILEGES
** . Granted priveleges are stored in pg tables *.relacl as an array of acl items. 
** . There may be 0 or more ACLs in the ACL array. i.e.:
**   {dba_role=arwdRxt/yellowbrick,"ro_role=r/yellowbrick","rw_role=arwd/yellowbrick"} 
** . aclitems array format:
**      grantee=privs/grantor 
**   WHERE -- Role granted to privileges granted to a role are one or more of the following
**                                                                                                        FUNCS
** Key SYSTEM               DATABASE        SCHEMAS    DEFAULTACLs   TABLES/VIEWS   COLUMNS    SEQUENCES &PROCS  ROLES      KEYS     MEMBERSHIP
** --- -------------------  --------------- ---------  ------------  ------------   ---------- --------- ------- ---------- -------- ----------
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
** !NOTE!
** . Membership is stored in pg_auth_members and does not have acl key                                                                                                                                 
** 
** See also: https://docs.yellowbrick.com/5.2.29/administration/abbrevs_access_privileges.html#abbreviations-for-acls
**
** Example:
**    arwdxt -- ALL PRIVILEGES (for tables)
**    *      -- grant option for preceding privilege
**
** datacl | {=Tc/kick,kick=awdCTcpqQuOZ/kick,sys_ybd_acl_grantor=a/kick}
**
** (c) 2018-2024 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** 2025-02-12 - Added ROLE and ON ROLE privileges. SP and table name change.
** 2025-01-30 - Fixed bug in Default Privileges and sequence. 
** 2024-05-16 - Updated help text.
** 2024-01-12 - Added _show_sql 
**            - Added roles to output, not just users.
** 2023.12.21 - Add sequences to output.
** 2023.12.19 - Fix bug in system perms
**              Add comma delimiter
**              Update privs table comment.
**              Remove default ACLs as there is no permissions function for them
**                and they are not objects users can access.
** 2023.12.18 - Initial version. Yellowbrick Technical Support 
**
** TODO: 
** . Clean up "obj_acls" CTE. Only needed in its current form for grants sp.
** . There does not appear to be a "has_*_privilege" for default ACLs.
** . There is not a has_role_privilege but there is pg_has_role. Should it be implemented?
*/

/* ****************************************************************************
**  Example results:
**
**   db_name   | obj_type  | schema_name |  obj_name  | owner_name  |  grantee    |                       grants ...
**-------------+-----------+-------------+------------+-------------+-------------+-----------------------------------------...
** yellowbrick | TABLE     | public      | a          | yellowbrick | yb100       |                                         ...
** yellowbrick | TABLE     | public      | ascii_test | ybdadmin    | ybtest      |                                         ...
** yellowbrick | UDF (C)   | public      | add_period | yellowbrick | ybtest      | EXECUTE                                 ...
** yellowbrick | VIEW      | public      | a3_v1      | ybdadmin    | ybtest      |                                         ...
** yellowbrick | TABLE     | public      | a          | yellowbrick | yellowbrick | INSERT WITH GRANT SELECT WITH GRANT UPDA...
** yellowbrick | UDF (C)   | public      | add_period | yellowbrick | yellowbrick | EXECUTE WITH GRANT                      ...
** yellowbrick | UDF (SQL) | public      | add_i      | yellowbrick | yellowbrick | EXECUTE WITH GRANT                      ...
** yellowbrick | VIEW      | public      | a3_v1      | ybdadmin    | yellowbrick | INSERT WITH GRANT SELECT WITH GRANT UPDA...
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/

DROP TABLE IF EXISTS db_obj_grants_t CASCADE;

CREATE TABLE         db_obj_grants_t
(
   db_name      VARCHAR (128)
 , obj_type     VARCHAR (25)
 , schema_name  VARCHAR (128)
 , obj_name     VARCHAR (128)
 , owner_name   VARCHAR (128) 
 , grantee      VARCHAR (128)
 , grants       VARCHAR (1024)
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE db_obj_grants_p(
     _user_ilike     VARCHAR
   , _schema_ilike   VARCHAR DEFAULT '%'
   , _obj_name_ilike VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE'
   , _show_sql       INTEGER DEFAULT 0   )
   RETURNS SETOF db_obj_grants_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER  
AS
$proc$
DECLARE

   _curr_user_sql       TEXT;
   _db_id               BIGINT;
   _db_info_sql         TEXT;
   _db_name             VARCHAR(128);
   _obj_pred            TEXT;   
   _sql                 TEXT;
   _user_info_sql       TEXT;
   
   _user_rec            RECORD;
   _fn_name             VARCHAR(256) := 'db_obj_grants_p';
   _prev_tags           VARCHAR(256) := current_setting('ybd_query_tags');
   _tags                VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END 
                                        || 'sysviews:' || _fn_name;       
   

BEGIN
 
   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags );
   --PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);
   
   -- Query for the databases to iterate over
   SELECT 
      database_id AS db_id, name AS db_name 
   INTO _db_id, _db_name
   FROM sys.database 
   WHERE db_name = current_database() 
   LIMIT 1
   ;
   --RAISE info '_db_id=%, _db_name=%', _db_id, _db_name;
   
   -- TODO update this so nulls are accounted for if ilike %
   _obj_pred := '   (    schema_name ILIKE ' || quote_literal( _schema_ilike   ) || ' 
                      OR schema_name IS NULL
                    )
                AND (    obj_name ILIKE ' || quote_literal( _obj_name_ilike    ) || '
                      OR obj_name ILIKE ' || '''' || _obj_name_ilike || '.%'''   || ' 
                      OR obj_name IS NULL
                    )
   ';
   --RAISE INFO '_obj_pred=%', _obj_pred;
      
   -- Query for the users to iterate over
   _user_info_sql = $$SELECT 
      oid     AS user_id
    , rolname AS user_name
   FROM pg_authid 
   WHERE user_name ILIKE $$ || quote_literal( _user_ilike  ) || $$
     AND user_name !~ 'backup_[a-f0-9]{8}'
   ORDER BY user_name
   $$;
   IF ( _show_sql > 0 ) THEN RAISE INFO '_user_info_sql = %', _user_info_sql; END IF;   

   /* Iterate over each user 
   */
   FOR _user_rec IN EXECUTE _user_info_sql 
   LOOP

      --RAISE INFO 'user_id=%, user_name=%', _user_rec.user_id, _user_rec.user_name ;

      _curr_user_sql := 'WITH owners AS
      (  SELECT ''USER'' AS owner_type
          , oid          AS owner_id
          , rolname      AS owner_name
         FROM pg_roles WHERE rolcanlogin = ''f''
         UNION ALL
         SELECT ''ROLE'' AS owner_type
          , oid          AS owner_id
          , rolname      AS owner_name
         FROM  pg_roles WHERE rolcanlogin = ''t''
      )
      
      , rels AS
      (  SELECT oid                                                  AS obj_id
       , relnamespace                                                AS schema_id
       , CASE relkind
            WHEN ''r'' THEN ''TABLE''::VARCHAR( 16 )
            WHEN ''v'' THEN ''VIEW''::VARCHAR( 16 )
         END                                                         AS obj_type
       , relname                                                     AS obj_name
       , relowner                                                    AS owner_id
       , IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''INSERT''                       ), '', INSERT''    , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''INSERT WITH GRANT OPTION''     ), '' WITH GRANT'' , '''')  
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''SELECT''                       ), '', SELECT''    , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''SELECT WITH GRANT OPTION''     ), '' WITH GRANT'' , '''')
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''UPDATE''                       ), '', UPDATE''    , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''UPDATE WITH GRANT OPTION''     ), '' WITH GRANT'' , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''DELETE''                       ), '', DELETE''    , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''DELETE WITH GRANT OPTION''     ), '' WITH GRANT'' , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''TRUNCATE''                     ), '', TRUNCATE''  , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''TRUNCATE WITH GRANT OPTION''   ), '' WITH GRANT'' , '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''REFERENCES''                   ), '', REFERENCES'', '''') 
       ||IIF( has_table_privilege(' || _user_rec.user_id || ', obj_id, ''REFERENCES WITH GRANT OPTION'' ), '' WITH GRANT'' , '''') 
                                                                     AS acl
      FROM  pg_catalog.pg_class
      WHERE  oid     >= 16384
         AND relkind IN( ''r'', ''v'' )
         AND relname NOT LIKE ''yb_deletes_%''
      LIMIT 100000
      )
      
      , seqs AS
      (  SELECT oid    AS obj_id
       , relnamespace  AS schema_id
       , ''SEQUENCE''::VARCHAR( 16 )   AS obj_type
       , relname  AS obj_name
       , relowner AS owner_id
       , IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''SELECT''                  ), '', SELECT''   , '''') 
   --    ||IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''SELECT WITH GRANT OPTION''), '' WITH GRANT'', '''') 
       ||IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''UPDATE''                  ), '', UPDATE''   , '''') 
   --    ||IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''UPDATE WITH GRANT OPTION''), '' WITH GRANT'', '''') 
       ||IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''USAGE''                   ), '', USAGE''    , '''') 
   --    ||IIF( has_sequence_privilege(' || _user_rec.user_id || ', obj_id, ''USAGE WITH GRANT OPTION'' ), '' WITH GRANT'', '''') 
                                                                     AS acl
      FROM  pg_catalog.pg_class
      WHERE  oid     >= 16384
         AND relkind IN( ''S'' )
      LIMIT 100000
      )
      
      , cols AS
      ( SELECT r.obj_id                                              AS obj_id
       , r.schema_id                                                 AS schema_id
       , r.obj_type || ''.COLUMN''::VARCHAR( 16 )                    AS obj_type
       , r.obj_name || ''.'' || a.attname                            AS obj_name
       , r.owner_id                                                  AS owner_id
       , IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''INSERT''                      ), '', INSERT''    , '''') 
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''INSERT WITH GRANT OPTION''    ), '' WITH GRANT'' , '''') 
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''SELECT''                      ), '', SELECT''    , '''')
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''SELECT WITH GRANT OPTION''    ), '' WITH GRANT'' , '''')       
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''UPDATE''                      ), '', UPDATE''    , '''') 
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''UPDATE WITH GRANT OPTION''    ), '' WITH GRANT'' , '''')
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''REFERENCES''                  ), '', REFERENCES'', '''')      
       ||IIF( has_column_privilege(' || _user_rec.user_id || ', r.obj_id, a.attnum, ''REFERENCES WITH GRANT OPTION''), '' WITH GRANT'' , '''')   
       
                                                                     AS acl

      FROM pg_catalog.pg_attribute AS a
      JOIN rels                    AS r ON a.attrelid = r.obj_id
      WHERE a.attacl IS NOT NULL
        AND a.attrelid >= 16384
        AND a.attnum   >  0
      )
      
      -- omit system created schemas except public.
      , schemas AS
      ( SELECT oid                 AS obj_id
       , oid                       AS schema_id
       , ''SCHEMA''::VARCHAR( 16 ) AS obj_type
       , nspname                   AS obj_name
       , nspowner                  AS owner_id
       , IIF( has_schema_privilege(' || _user_rec.user_id || ', obj_id, ''USAGE''                     ), '', USAGE''     , '''') 
       ||IIF( has_schema_privilege(' || _user_rec.user_id || ', obj_id, ''USAGE WITH GRANT OPTION''   ), '' WITH GRANT'' , '''') 
       ||IIF( has_schema_privilege(' || _user_rec.user_id || ', obj_id, ''CREATE''                    ), '', CREATE''    , '''') 
       ||IIF( has_schema_privilege(' || _user_rec.user_id || ', obj_id, ''CREATE WITH GRANT OPTION''  ), '' WITH GRANT'' , '''') 
                                                                     AS acl
      FROM  pg_catalog.pg_namespace
      WHERE(   oid = 2200
            OR oid >= 16384 
           )
         AND nspname NOT LIKE ''pg_t%''
      )
      
      , procs AS
      ( SELECT p.oid    AS obj_id
       , p.pronamespace AS schema_id
       , CASE l.lanname
            WHEN ''plpgsql'' THEN ''PROCEDURE''::VARCHAR( 16 )
            WHEN ''ybcpp''   THEN ''UDF (C)''::VARCHAR( 16 )
            WHEN ''sql''     THEN ''UDF (SQL)''::VARCHAR( 16 )
         END              AS obj_type
       , proname          AS obj_name
       , proowner         AS owner_id
       , IIF( has_function_privilege(' || _user_rec.user_id || ', obj_id, ''EXECUTE''                   ), '', EXECUTE''  , '''') 
       ||IIF( has_function_privilege(' || _user_rec.user_id || ', obj_id, ''EXECUTE WITH GRANT OPTION'' ), '' WITH GRANT'', '''') 
      FROM pg_catalog.pg_proc     AS p
      JOIN pg_catalog.pg_language AS l ON p.prolang = l.oid
      WHERE p.oid > 16384 
         AND l.lanname IN( ''plpgsql'', ''ybcpp'', ''sql'' )
      )
      
      /* TODO: There does not appear to be a has_*_privilege for default ACLs */
      , dflt_acls AS            
      ( 
		SELECT 
		obj_id
		,schema_id
		,obj_type
		,object_name
		,owner_id
		,string_agg(acl, '', '') as concat_acl
		FROM	 
			(SELECT
				pda.oid AS obj_id
				,pda.defaclnamespace     AS schema_id
				,''ALTER DEFAULT PRIVILEGES'' AS obj_type
				,CASE pda.defaclobjtype
						WHEN ''r'' THEN ''TABLE''
						WHEN ''S'' THEN ''SEQUENCE''
						WHEN ''f'' THEN ''FUNCTION''
						WHEN ''p'' THEN ''PROCEDURE''
						WHEN ''T'' THEN ''TYPE''
						ELSE ''UNKNOWN''
					END AS object_name,
				defaclrole AS owner_id,
				CASE WHEN acl.is_grantable is true THEN privilege_type || '' WITH GRANT''
					WHEN acl.is_grantable is false THEN privilege_type END AS acl,
				grantee.rolname AS grantee
			FROM pg_default_acl pda
			JOIN pg_namespace ON pg_namespace.oid = pda.defaclnamespace,
				aclexplode(defaclacl) AS acl
			JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
			WHERE grantee.oid = ' || _user_rec.user_id || '
			) adp
		GROUP BY obj_id, schema_id, obj_type, object_name, owner_id, grantee
      )
      
      , dbs AS
      ( SELECT oid                    AS obj_id
       , NULL::INT4                   AS schema_id
       , ''DATABASE''::VARCHAR( 16 )  AS obj_type
       , datname                      AS obj_name
       , datdba                       AS owner_id
       , IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''BULK LOAD''                         ), '', BULK LOAD''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''BULK LOAD WITH GRANT OPTION''       ), '' WITH GRANT''       , '''')       
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''ALTER ANY SCHEMA''                  ), '', ALTER ANY SCHEMA'', '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''ALTER ANY SCHEMA WITH GRANT OPTION''), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''DROP ANY SCHEMA''                   ), '', DROP ANY SCHEMA'' , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''DROP ANY SCHEMA WITH GRANT OPTION'' ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CREATE''                            ), '', CREATE''          , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CREATE WITH GRANT OPTION''          ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''TEMPORARY''                         ), '', TEMPORARY''       , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''TEMPORARY WITH GRANT OPTION''       ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CONNECT''                           ), '', CONNECT''         , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CONNECT WITH GRANT OPTION''         ), '' WITH GRANT''       , '''')       
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''EXPLAIN QUERY''                     ), '', EXPLAIN QUERY''   , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''EXPLAIN QUERY WITH GRANT OPTION''   ), '' WITH GRANT''       , '''')        
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''VIEW QUERY TEXT''                   ), '', VIEW QUERY TEXT'' , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''VIEW QUERY TEXT WITH GRANT OPTION'' ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''TRACE QUERY''                       ), '', TRACE QUERY''     , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''TRACE QUERY WITH GRANT OPTION''     ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''BACKUP''                            ), '', BACKUP''          , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''BACKUP WITH GRANT OPTION''          ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''RESTORE''                           ), '', RESTORE''         , '''') 
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''RESTORE WITH GRANT OPTION''         ), '' WITH GRANT''       , '''')
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CONTROL''                           ), '', CONTROL''         , '''')       
       ||IIF( has_database_privilege(' || _user_rec.user_id || ', obj_id, ''CONTROL WITH GRANT OPTION''         ), '' WITH GRANT''       , '''') 
                                      AS acl
      FROM  pg_catalog.pg_database
      WHERE datname = current_database()
      )   
      
      , encrypt_keys AS
      ( SELECT k.oid                  AS obj_id
       , k.keynamespace               AS schema_id
       , ''KEY''::VARCHAR( 16 )       AS obj_type
       , k.keyname                    AS obj_name
       , k.keyowner                   AS owner_id
       , IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''ENCRYPT''                   ), '', ENCRYPT''   , '''') 
       ||IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''ENCRYPT WITH GRANT OPTION'' ), '' WITH GRANT'' , '''') 
       ||IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''DECRYPT''                   ), '', DECRYPT''   , '''') 
       ||IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''DECRYPT WITH GRANT OPTION'' ), '' WITH GRANT'' , '''') 
       ||IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''HMAC''                      ), '', HMAC''      , '''')
       ||IIF( has_key_privilege(' || _user_rec.user_id || ', obj_id, ''HMAC WITH GRANT OPTION''    ), '' WITH GRANT'' , '''')

                                                                     AS acl
      FROM      pg_catalog.pg_keystore  AS k       
      LEFT JOIN pg_catalog.pg_namespace AS n ON k.keynamespace = n.oid
      WHERE k.keydatabase = ' || _db_id || '
      )   
      
      , onroles AS
      ( SELECT r.oid                  AS obj_id
       , NULL::INT4                   AS schema_id
       , CASE WHEN r.rolcanlogin THEN ''USER''
              ELSE ''ROLE''
         END::VARCHAR( 16 )           AS obj_type
       , r.rolname                   AS obj_name
       , NULL::INT4                   AS owner_id
       , IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''ALTER ROLE''                   ), '', ALTER ROLE''   , '''') 
       ||IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''ALTER ROLE WITH GRANT OPTION'' ), '' WITH GRANT'' , '''') 
       ||IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''DROP ROLE''                   ), '', DROP ROLE''   , '''') 
       ||IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''DROP ROLE WITH GRANT OPTION'' ), '' WITH GRANT'' , '''') 
       ||IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''CONTROL''                      ), '', CONTROL''      , '''')
       ||IIF( has_role_privilege(' || _user_rec.user_id || ', obj_id, ''CONTROL WITH GRANT OPTION''    ), '' WITH GRANT'' , '''')

                                                                     AS acl
      FROM pg_catalog.pg_authid  AS r    
      WHERE rolacl IS NOT NULL
      )
	  
	  , roles AS 
	  ( 	  SELECT
           m.oid  AS obj_id
         , NULL::INT4 AS schema_id
         , ''MEMBERSHIP''::VARCHAR( 16 ) AS obj_type
		 , o.rolname AS obj_name
         , o.oid          AS owner_id
      --   , ''GRANT '' || o.rolname || '' TO '' || m.rolname || CASE WHEN am.admin_option is true then '' WITH GRANT OPTION;'' ELSE '''' END AS  acl
	     ,  CASE WHEN am.admin_option is true then '' WITH ADMIN OPTION'' ELSE '''' END AS  acl
	  FROM
           pg_auth_members am
      INNER JOIN pg_roles o
           ON o.oid = am.roleid
      INNER JOIN pg_roles g
           ON g.oid = am.grantor
      INNER JOIN pg_roles m
           ON m.oid = am.member
	  WHERE m.oid = ' || _user_rec.user_id || '
      )
	  
      , system AS
      ( SELECT NULL::oid              AS obj_id
       , NULL::INT4                   AS schema_id
       , ''SYSTEM''::VARCHAR( 16 )    AS obj_type
       , ''SYSTEM''::VARCHAR( 16 )    AS obj_name
       , NULL::INT4                   AS owner_id
       , IIF( has_system_privilege(' || _user_rec.user_id || ', ''ALTER ANY DATABASE''                    ), '', ALTER ANY DATABASE''  , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''ALTER ANY DATABASE WITH GRANT OPTION''  ), '' WITH GRANT''           , '''')
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''ALTER ANY ROLE''                        ), '', ALTER ANY ROLE''      , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''ALTER ANY ROLE WITH GRANT OPTION''      ), '' WITH GRANT''           , '''')
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''BACKUP ANY DATABASE''                   ), '', BACKUP ANY DATABASE'' , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''BACKUP ANY DATABASE WITH GRANT OPTION'' ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CONTROL ANY SESSION''                   ), '', CONTROL ANY SESSION'' , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CONTROL ANY SESSION WITH GRANT OPTION'' ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CONTROL LDAP''                          ), '', CONTROL LDAP ''       , '''')
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CONTROL LDAP WITH GRANT OPTION''        ), '' WITH GRANT''           , '''')        
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CREATE DATABASE''                       ), '', CREATE DATABASE''     , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CREATE DATABASE WITH GRANT OPTION''     ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CREATE ROLE''                           ), '', CREATE ROLE''         , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''CREATE ROLE WITH GRANT OPTION''         ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''DROP ANY DATABASE''                     ), '', DROP ANY DATABASE''   , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''DROP ANY DATABASE WITH GRANT OPTION''   ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''DROP ANY ROLE''                         ), '', DROP ANY ROLE''       , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''DROP ANY ROLE WITH GRANT OPTION''       ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''EXPLAIN QUERY''                         ), '', EXPLAIN QUERY''       , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''EXPLAIN QUERY WITH GRANT OPTION''       ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''RESTORE ANY DATABASE''                  ), '', RESTORE ANY DATABASE'', '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''RESTORE ANY DATABASE WITH GRANT OPTION''), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''TRACE QUERY''                           ), '', TRACE QUERY''         , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''TRACE QUERY WITH GRANT OPTION''         ), '' WITH GRANT''           , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''VIEW QUERY TEXT''                       ), '', VIEW QUERY TEXT''     , '''') 
     /*||IIF( has_system_privilege(' || _user_rec.user_id || ', ''VIEW QUERY TEXT WITH GRANT OPTION''     ), '' WITH GRANT''           , '''')*/
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''VIEW ROLE''                             ), '', VIEW ROLE ''          , '''') 
       ||IIF( has_system_privilege(' || _user_rec.user_id || ', ''VIEW ROLE WITH GRANT OPTION''           ), '' WITH GRANT''           , '''') 
      )                                                                                                         
    
      , objs AS
      ( 
                     SELECT *   FROM dbs
         UNION ALL   SELECT *   FROM rels   
         UNION ALL   SELECT *   FROM seqs
         UNION ALL   SELECT *   FROM cols
         UNION ALL   SELECT *   FROM procs
         UNION ALL   SELECT *   FROM schemas
         UNION ALL   SELECT *   FROM system  
         UNION ALL   SELECT *   FROM dflt_acls
		 UNION ALL   SELECT *   FROM roles 
		 UNION ALL   SELECT *   FROM onroles
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
            WHEN obj_type NOT IN ( ''DATABASE'', ''SCHEMA'', ''ALTER DEFAULT PRIVILEGES'' ) THEN n.nspname || ''.''
            ELSE ''''
         END
         || o.obj_name          AS fqn
       , o.owner_id             AS owner_id
       , u.owner_type           AS owner_type
       , u.owner_name           AS owner_name
       , o.acl                  AS acl
      FROM objs                                                            AS o
         LEFT JOIN pg_catalog.pg_namespace AS n ON o.schema_id = n.oid
         LEFT JOIN owners                                                  AS u ON o.owner_id  = u.owner_id
      )
      ,obj_acls AS
      (
         SELECT 
           db_name::VARCHAR(128)          AS db_name
         , obj_type::VARCHAR(25)          AS obj_type
         , schema_name::VARCHAR(128)      AS schema_name
         , obj_name::VARCHAR(128)         AS obj_name
         , owner_name::VARCHAR(128)       AS owner_name
         , acl::VARCHAR(1024)             AS acl     
         FROM obj_attrs
         WHERE acl IS NOT NULL   
           AND ' || _obj_pred       || '   
		   AND ' || _yb_util_filter || '    


         UNION ALL 
         SELECT 
           db_name::VARCHAR(128)          AS db_name
         , obj_type::VARCHAR(25)          AS obj_type
         , schema_name::VARCHAR(128)      AS schema_name
         , obj_name::VARCHAR(128)         AS obj_name
         , owner_name::VARCHAR(128)       AS owner_name
         , NULL::VARCHAR( 1024 )          AS acl     
  
         FROM obj_attrs
         WHERE acl IS NULL
           AND ' || _obj_pred       || '
           AND ' || _yb_util_filter || '    

      )
      , obj_grants AS
      (
         SELECT 
           db_name::VARCHAR(128)                                                                   AS db_name
         , obj_type::VARCHAR(25)                                                                   AS obj_type
         , schema_name::VARCHAR(128)                                                               AS schema_name
         , obj_name::VARCHAR(128)                                                                  AS obj_name
         , owner_name::VARCHAR (128)                                                               AS owner_name 
         , ' || quote_literal( _user_rec.user_name ) || '::VARCHAR (128)                           AS grantee   
         , LTRIM(acl, '', '')::VARCHAR(1024)                                                       AS acl

         FROM obj_acls     
      )
      SELECT * 
      FROM obj_grants
      ORDER BY obj_type, schema_name, obj_name 
      '
      ;

      IF ( _show_sql > 0 ) THEN RAISE INFO '_curr_user_sql=%', _curr_user_sql; END IF;
      RETURN QUERY EXECUTE _curr_user_sql;

   END LOOP;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );

END;
$proc$
;


COMMENT ON FUNCTION db_obj_grants_p(VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER) IS 
$cmnt$Description:
User permissions on user objects in the current databases with owner detail.

Includes system, databases, schemas, tables, views, sequences, stored procedures
   ,columns, roles, membership, and keys.
Excludes system tables/views/functions, triggers, and default ACL objects.
  
Examples:
  SELECT * FROM db_obj_grants_p('dev_user1') 
  SELECT * FROM db_obj_grants_p('dev_user%') WHERE obj_type = 'SCHEMA'  
  SELECT * FROM db_obj_grants_p( 'dbadmin_r', '%', '%fact%' );
  SELECT * FROM db_obj_grants_p( 'dbadmin_r', 'public', _show_sql:=1 );
  
Arguments:
. _user_ilike     VARCHAR (optl)   - An ILIKE pattern for the user name(s). Default is '%'.
                                     Apply a pattern to avoid massive front end results.
. _schema_ilike   VARCHAR (optl)   - An ILIKE pattern for the object schema name. Default is '%'.
. _obj_name_ilike VARCHAR (optl)   - An ILIKE pattern for the object name. Default is '%'.
. _yb_util_filter VARCHAR (intrnl) - Used by YbEasyCli.
. _show_sql       VARCHAR (optl)   - Set to > 0 to see generated sql. Default is 0.

Usage Notes:
. WARNING: if you run this for all users for all objects you can run your 
  manager node out of disk catalog space as the massive join will spill to temp
  space that is under the catalog directory.
. If a user has no privileges on an object, the "grants" column value
  will be NULL.
. Objects not belonging to a schema (like databases and default ACLs) are always 
  returned even if a schema filter is applied. 
. This procedure is database specific; it does not work in a cross-database
  context.
. If the owner of the user objects is the grantee, then WITH GRANT OPTION is automatically applied.

Example Results:
   db_name   | obj_type  | schema_name |  obj_name  | owner_name  |  grantee    |                       grants ...
-------------+-----------+-------------+------------+-------------+-------------+-----------------------------------------...
 yellowbrick | TABLE     | public      | a          | yellowbrick | yb100       |                                         ...
 yellowbrick | TABLE     | public      | ascii_test | ybdadmin    | ybtest      |                                         ...
 yellowbrick | UDF (C)   | public      | add_period | yellowbrick | ybtest      | EXECUTE                                 ...
 yellowbrick | VIEW      | public      | a3_v1      | ybdadmin    | ybtest      |                                         ...
 yellowbrick | TABLE     | public      | a          | yellowbrick | yellowbrick | INSERT WITH GRANT, SELECT WITH GRANT, UPDA...
 yellowbrick | UDF (C)   | public      | add_period | yellowbrick | yellowbrick | EXECUTE WITH GRANT                      ...
 yellowbrick | UDF (SQL) | public      | add_i      | yellowbrick | yellowbrick | EXECUTE WITH GRANT                      ...
 yellowbrick | VIEW      | public      | a3_v1      | ybdadmin    | yellowbrick | INSERT WITH GRANT, SELECT WITH GRANT, UPDA...

Version:
. 2025.02.12 - Yellowbrick Technical Support
$cmnt$
;


