/* sysviews_create.sql
**
** Simple ybsql script to run sysview procedure scipts to:
** . set the default search_path to PUBLIC, and pg_catalog
** . create the procedures
** . output an empty row for each prodedure
**
** To grant default permissions on the procedures, run sysviews_grant.sql .
** To test the procedures, run sysviews_test.sql .
**
** Version history:
** 2021.05.11
** 2021.04.22
** 2021.04.09
** 2021.01.20
** 2020.11.09
** 2020.10.31
** 2020.07.29
** 2020.06.15
** 2020.04.25
** 2020.02.09
** 2021.11.18 
** 2022.03.27 
*/

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
SET search_path TO public,pg_catalog;
CREATE DATABASE sysviews ENCODING UTF8;
\c sysviews

SELECT LEFT( setting, 1 ) AS ver_m FROM pg_settings WHERE name = 'yb_server_version' ;
\gset

\echo
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo Running stored proc creation scripts for major version :ver_m

\echo Create the sysviews_settings table
\i  sysviews_settings.sql

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo Create the stored procedures
\i  all_user_objs_p.sql
\i  analyze_immed_user_p.sql
\i  analyze_immed_sess_p.sql
\i  bulk_xfer_p.sql
\i  column_dstr_p.sql
\i  column_stats_p.sql
\i  help_p.sql
\i  log_query_p.sql
\i  log_query_pivot_p.sql
\i  log_query_smry_p.sql
\i  log_query_steps_p.sql
\i  procedure_p.sql
\i  query_p.sql
\i  query_rule_events_p.sql
\i  query_steps_p.sql
\i  rel_p.sql
\i  rowstore_p.sql
\i  rowstore_by_table_p.sql
\i  schema_p.sql
\i  session_p.sql
\i  session_smry_p.sql
\i  sql_inject_check_p.sql
\i  storage_by_db_p.sql
\i  storage_by_schema_p.sql
\i  storage_by_table_p.sql
\i  storage_p.sql
\i  sysviews_p.sql
\i  table_constraints_p.sql
\i  table_skew_p.sql
-- \i  'table_deps_p_v':ver_m'.sql'
\i  view_ddls_p.sql
\i  wlm_active_profile_p.sql
\i  wlm_active_rule_p.sql
\i  wlm_state_p.sql
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo
\q 
