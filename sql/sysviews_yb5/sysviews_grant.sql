/* sysviews_grant.sql
**
** Simple ybsql script for default GRANTs on the sysviews procedures and tables.
**
** Version history:
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.04.25
*/

SET search_path TO public,pg_catalog;

\c sysviews

\echo
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

\echo REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;
REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;

\echo GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA PUBLIC TO PUBLIC;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA PUBLIC TO PUBLIC;

\echo GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO PUBLIC;

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo