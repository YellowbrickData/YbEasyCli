/* sysviews_grant.sql
**
** Create sysview_users_role and GRANTs on the sysviews procedures and tables.
**
** Version history:
** . 2022.08.29 - Cosmetic update role membership output
** . 2022.07.08 - Added role membership output
** . 2021.12.22 - Add sysviews_users_r (role)
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.04.25 - YB Technical Support
*/

SET search_path TO public,pg_catalog;

\c sysviews

\echo
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

\echo CREATE ROLE sysviews_users_r;
CREATE ROLE sysviews_users_r;

\echo REVOKE CONNECT ON DATABASE sysviews FROM PUBLIC;
REVOKE CONNECT ON DATABASE sysviews FROM PUBLIC;

\echo GRANT CONNECT ON DATABASE sysviews TO sysviews_users_r;
GRANT CONNECT ON DATABASE sysviews TO sysviews_users_r;

\echo REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;
REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;

\echo GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA PUBLIC TO PUBLIC;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA PUBLIC TO PUBLIC;

\echo GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO PUBLIC;

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo sysviews_users_r role members:
SELECT
   user_id  user_or_role_id
 , name     user_or_role_name
FROM
   sys.user
WHERE
   pg_has_role (user_id, 'sysviews_users_r', 'member')
   AND user_id >= 16384 
ORDER BY
   name
; 


\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo