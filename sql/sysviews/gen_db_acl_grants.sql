/* gen_db_acl_grants.sql
**
** Generate GRANT and REVOKE statements for an existing database object. 
**
** Description:
** This is often needed when migrating databases from one appliance to another
** because the permissions on the database itself are not restored.
**
** The only GRANT/REVOKE'able permissions on a database are:
** . CREATE     C    
** . CONNECT    c   
** . TEMPORARY  T   
**
** Caveats:
** . While pg_database is global, THIS QUERY EXPLLICITLY GETS ONLY THOSE GRANTS
**   FOR THE CURRENT DB.
**
** Version history:
** . 2020-05-29 (rek)
*/

/* Example results 

/* =T/yellowbrick ...
REVOKE ALL ON denav FROM PUBLIC ;
GRANT TEMPORARY ON DATABASE denav TO PUBLIC ;

/* denav_group=c/yellowbrick ...
GRANT CONNECT ON DATABASE denav TO denav_group ;

/* john=c/yellowbrick * /
GRANT CONNECT ON DATABASE denav TO john ;

/* yellowbrick=CTc/yellowbrick ...
GRANT CREATE, TEMPORARY, CONNECT ON DATABASE denav TO yellowbrick ;

*/


WITH db_acls AS
   (  SELECT
         d.datname                         AS name
       , unnest (d.datacl) ::varchar (300) AS access_privs
      FROM
         pg_catalog.pg_database d
   )
 , db_acl_parts AS
   (  SELECT
         name
       , CASE
            WHEN LEFT (access_privs, 1) = '=' THEN 'default'
            ELSE 'role'
         END AS type
       , access_privs::varchar (256)
       , split_part (access_privs, '=', 1)                      AS role
       , split_part (split_part (access_privs, '=', 2), '/', 1) AS perms
       , split_part (access_privs, '/', 2)                      AS rule
      FROM
         db_acls
   )
SELECT
   '/* ' || access_privs || ' */' || E'\n' ||
   CASE
      WHEN type = 'default' THEN 'REVOKE ALL ON ' || name || ' FROM PUBLIC ;' || E'\n'
      ELSE ''
   END || 
   'GRANT ' ||
   CASE perms
      WHEN 'CTc' THEN 'CREATE, TEMPORARY, CONNECT'
      WHEN 'T' THEN 'TEMPORARY'
      WHEN 'Tc' THEN 'TEMPORARY, CONNECT'
      WHEN 'c' THEN 'CONNECT'
   END || ' ON DATABASE ' || name || ' TO ' ||
   CASE
      WHEN type = 'role' THEN role
      ELSE 'PUBLIC'
   END || ' ;'  || E'\n' AS grant_sql
   
FROM
   db_acl_parts
WHERE
   name = current_database()
ORDER BY
   name
 , type
 , role
;