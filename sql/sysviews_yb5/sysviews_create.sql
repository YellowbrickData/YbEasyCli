/* sysviews_create.sql
**
** ybsql script for Yellowbrick Version 5 to run sysview procedure DDLs to:
** . set the default search_path to PUBLIC, and pg_catalog
** . create the procedures
**
** To grant default permissions on the procedures, run sysviews_grant.sql .
**
** Version history:         
** . 2023.06.05 - Re-added backup_chains_p.sql 
** . 2023.05.15 - Added backup_chains_p.sql 
** . 2023.03.20 - Added log_query_smry_by_p.sql 
** . 2023.03.10 - Added rel_ddl.sql
** . 2022.12.28 - Added:
**                  catalog_storage_by_db_p.sql    
**                  column_p.sql                                    
** .                catalog_storage_by_db_p.sql    
** .                catalog_storage_by_table_p.sql 
** .                log_replica_p.sql              
** .                stmt_topn_p.sql                
** .                storage_by_worker_p.sql        
** .                table_p.sql                                        
** .                version_p.sql                  
** .                view_validate_p.sql                
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.07 - Yellowbrick Technical Support
** . 2021.05.07 - For Yellowbrick version >= 5.0 only.
** . 2022.03.27 - added query_rule_events_p.sql
** . 2022.10.08 - added log_query_slot_usage_p.sql
*/

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
SET search_path TO public,pg_catalog;
CREATE DATABASE sysviews ENCODING UTF8;

\c sysviews
SET search_path TO public,pg_catalog;                     

SELECT LEFT( setting, 1 ) AS yb_major_ver FROM pg_settings WHERE name = 'yb_server_version' ;
\gset

\echo
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                                                                             
\echo Create the sysviews_settings table
\i  sysviews_settings.sql

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo Create the stored procedures
\i  sql_inject_check_p.sql
\i  all_user_objs_p.sql
\i  analyze_immed_user_p.sql
\i  analyze_immed_sess_p.sql
\i  backup_chains_p.sql 
\i  bulk_xfer_p.sql
\i  catalog_storage_by_table_p.sql 
\i  catalog_storage_by_db_p.sql                             
\i  column_dstr_p.sql
\i  column_p.sql
\i  column_stats_p.sql
--\i  column_values_p.sql
--\i  db_role_privs_p.sql
\i  help_p.sql
\i  load_p.sql
\i  lock_p.sql
\i  log_bulk_xfer_p.sql
\i  log_query_p.sql
\i  log_query_pivot_p.sql
\i  log_query_slot_usage_p.sql
\i  log_query_smry_p.sql
\i  log_query_smry_by_p.sql
\i  log_query_steps_p.sql
\i  log_query_timing_p.sql
\i  log_replica_p.sql
\i  procedure_p.sql
\i  query_p.sql
\i  query_rule_events_p.sql
\i  query_steps_p.sql
\i  rel_p.sql
\i  rel_ddl_p.sql
\i  replica_bulk_action_p.sql
\i  rowstore_by_table_p.sql
\i  rowstore_p.sql
\i  schema_p.sql
\i  session_p.sql
\i  session_smry_p.sql
\i  stmt_topn_p.sql
\i  storage_by_db_p.sql
\i  storage_by_schema_p.sql
\i  storage_by_table_p.sql
                           
\i  storage_p.sql
\i  sysviews_p.sql
\i  table_constraints_p.sql
\i  table_p.sql
--\i  table_info_p.sql
\i  table_skew_p.sql
--\i  table_skew2_p.sql
--\i  table_deps_p.sql
--\i  view_ddls_p.sql
\i  version_p.sql
\i  view_validate_p.sql            
\i  wlm_active_profile_p.sql
\i  wlm_active_rule_p.sql
\i  wlm_profile_rule_p.sql
\i  wlm_profile_sql_p.sql
\i  wlm_state_p.sql
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo
\q 
