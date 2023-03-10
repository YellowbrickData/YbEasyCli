README.TXT

Yellowbrick Data Technical Support sysviews version 5 README file.
Revision 2023.03.09

In this README
========================================
. NOTICE
. About the procedures
. Creating the procedures
. Using the procedures
. Technical implementation details
. Revision history


NOTICE
========================================
(c) 2018 - 2021 Yellowbrick Data Corporation.
. These scripts are provided free of charge by Yellowbrick Data Corporation as a
  convenience to its customers.

. These script are provided "AS-IS" with no warranty whatsoever.

. The customer accepts all risk in connection with the use of this script, and
  Yellowbrick Data Corporation shall have no liability whatsoever.

. These procedures are meant to be created using SECURITY DEFINER as a superuser.
  Consider the access you wish to allow to these

. The procedures do not contain query results. However, some procedures contain
  some portion of executed SQL statements which could possibly contain PII data.
  If this is of concern, check the procedure output and control access to the
  procedures accordingly.


About the procedures
================================================================================
. The sysviews are a collection of stored procedures to wrap commonly executed
  queries used in current and historical appliance, database, workload, and
  statement analysis.

. These procedures are for Yellowbrick version 3.x and 4.x. The sysviews_create
  SQL script will install the version of the stored procedures appropraite for
  your verion of YBDW.

. The procedures predominatly are named begining with the primary sys view or
  table they query and are all suffixed with _p. For example, query_P() is
  a wrapper around sys.query. log_query_steps() queries sys.log_query and joins
  additional query plan node/step information.

. To see
.. all the procedures and their arguments, do: SELECT * FROM sysviews_p();
.. help text on all the procedures, do       : SELECT * FROM help_p();
.. the schema of the returned results, do    : \d public.*_t;


Creating the procedures
================================================================================
. A simple SQL script (sysviews_create.sql) is provided to create a sysviews
  database and run tthe scripts to create the stored procedures.

. It is expected to be run as a superuser. See "Technical implementation details"
  further below.

. Procedures are created in the public schema of the database "sysviews". Edit
  the sysviews_create.sql file to change that.

. By default, no grants are applied to any of the procedures. The included file
  sysviews_grants.sql will GRANT EXECUTE privileges for non-superusers.
  i.e.:
     GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA PUBLIC TO PUBLIC;
     GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO PUBLIC;


Using the procedures
================================================================================
. All the procedures return a SET OF result set so must be invoked with SELECT.
  i.e. SELECT * FROM sysviews_p().

. The procedures included in this version follow and can be viewed in your
  sysviews database by running the procedure:
    SELECT procedure, description FROM sysviews_p();

         procedure          |                                        description
----------------------------+--------------------------------------------------------------------------------------------
 all_user_objs_p            | All user objects in all databases with owner and ACL detail.
 analyze_immed_sess_p       | SETs ybd_analyze_after_writes TO [OFF|ON]` for session as a superuser.
 analyze_immed_user_p       | Run ALTER USER session_user SET ybd_analyze_after_writes as a superuser.
 bulk_xfer_p                | Transformed subset active bulk transfers (ybload & ybunload) from sys.load and sys.unload.
 catalog_storage_by_db_p    | Size of catalog tables across all databases by database.\r
 catalog_storage_by_table_p | Size of catalog tables across all databases.\r
 column_dstr_p              | Distribution of rows per distinct values for column grouped on a logarithmic scale
 column_p                   | Table column information similar to "\d" but with additional metadata and for
 column_stats_p             | Table column metadata including cardinality estimates from the db statistics.
 help_p                     | Returns usage information on sys *_p procedures.
 load_p                     | Transformed subset of sys.load columns for currently active bulk loads.
 log_bulk_xfer_p            | Completed bulk transfers (ybload & ybunload) from sys.log_load and sys.log_unload.
 log_query_p                | Details on completed statements.
 log_query_pivot_p          | Queries for the last week aggregated by hour for use in WLM pivot table analysis.
 log_query_slot_usage_p     | Create a WLM slot usage by pool report by analyzing sys.log_query data.
 log_query_smry_p           | Aggregated subset of the sys.log_query data.
 log_query_steps_p          | Completed statements actual vs plan metrics by plan node.
 log_query_timing_p         | Details on completed backend statements.
 log_replica_p              | Currently running and historical replication statements.
 procedure_p                | User created stored procedures.
 query_p                    | Transformed subset of sys.query columns for currently running statements.
 query_rule_events_p        | Return the WLM rule events for a query.
 query_steps_p              | Currently executing statements actual vs plan metrics by plan node.
 rel_ddl_p                  | Generates a ddl for user table(s) as sequetial varchar rows.
 rel_p                      | All user "relations" (tables, views, & sequences) in all user databases.
 rowstore_by_table_p        | Size of rowstore data in user tables across all databases.\r
 rowstore_p                 | Rowstore aggregated metrics including rows and size of data.\r
 schema_p                   | All user schemas across all databases.
 session_p                  | Current session state details.
 session_smry_p             | Current sessions aggregated by db, user, state, app, ip, etc...
 sql_inject_check_p         | Check the input SQL clause for possible SQL injection.
 stmt_topn_p                | The top <n> (i.e. worst) performing statements across multiple columns.
 storage_by_db_p            | Storage space of committed blocks in user tables aggregated by database.
 storage_by_schema_p        | Storage summary by schema across one or more databases.
 storage_by_table_p         | Storage summary for user tables by database, schema, and table.
 storage_p                  | Aggregated appliance storage for data, spill, to GC, other, and total space.
 sysviews_p                 | Names and arguments for all installed sysviews procedures.
 table_constraints_p        | Existing constraints on user tables as per information_schema.table_constraints.
 table_p                    | All user tables across all databases metadata similar to ybsql "\d".
 table_skew_p               | Row and storage skew summary for user tables by database, schema, and table with
 table_skew_p               | Table skew report.
 version_p                  | The current installed sysviews_yb5 version.
 view_validate_p            | to late bound views.
 wlm_active_profile_p       | Returns current active WLM profile configuration details by pool.
 wlm_active_rule_p          | Current active WLM profile rules.
 wlm_profile_rule_p         | Current active or named WLM detailed profile rules.
 wlm_state_p                | Returns current active WLM profile state metrics by pool.
(47 rows)


Technical implementation details
================================================================================
. By default, all scripts are created with SECURITY DEFINER. This enables users
  to view the data as if they were a superuser. i.e. not only their own queires,
  tables, etc... To make it so that users can see only their own objects,
  queries, etc... remove SECURITY DEFINER.

. Yellowbrick does not support user defined types or RETURNS TABLE. Therefore,
  a table with the same name as the procdure execpt with the suffix "_t" is
  created to define the rowtype that will be returned by the procedure.

. All the procedures append "sysviews:<procedure_name>" to the ybd_query_tags
  property for the lifespan of the procedure.


Revision history
================================================================================

2023.03.10 
-------------------
rel_ddl_p.sql                  : Added 
stmt_topn_p.sql                : Updated to include db_name in output.
storage_by_db_p.sql            : Updated with new _db_ilike param.
sysviews_create.sql            : Updated with new rel_ddl_p.sql.
version_p.sql                  : Updated to include min and max major.minor release.


2023.01.20 
-------------------
log_bulk_xfer_p                : fix COMMENT ON
wlm_active_rule_p.sql          : fix COMMENT ON
log_query_slot_usage_p.sql     : fix COMMENT ON
                               
                               
2023.01.11
-------------------
rowstore_p.sql                 : Updated
sysviews_create.sql            : Updated
sysviews_p.sql                 : Updated
storage_by_table_p.sql         : Updated
storage_p.sql                  : Updated
session_smry_p.sql             : Updated
session_p.sql                  : Updated
sysviews_grant.sql             : Updated
wlm_active_profile_p.sql       : Updated
wlm_active_rule_p.sql          : Updated
                               
2022.12.28 
-------------------
catalog_storage_by_db_p.sql    : Added
catalog_storage_by_table_p.sql : Added
column_p.sql                   : Added
column_values_p.sql            : Added
log_replica_p.sql              : Added
stmt_topn_p.sql                : Added
storage_by_worker_p.sql        : Added
table_p.sql                    : Added
view_validate_p.sql            : Added
                               

2022.10.09
-------------------
column_p.sql                   : Fixed double-quoted db name issue.
column_values_p.sql            : Fixed double-quoted db name issue.


2022.09.21
-------------------
storage_by_worker_p.sql        : Added proc.
storage_p.sql                  : Fixed to_gc value.


2022.08.28
-------------------
column_values_p.sql            : Added internal ybutil filter.
log_query_p.sql                : Added db_name column.
                                 Added _date_part arg for date_part truncation. 
                                 Changed args order.
                                 Added restart_number "n"
log_query_pivot_p.sql          : Added requested memory column.
log_query_pivot_v5.xlsx        : Updated to include new requested memory column.
log_query_steps_p              : Fixed join problem.
                                 added additional columns.
                                 added custom option. 
                                 added NVL (detail). 
log_replica_.sql               : Added proc.
query_p.sql                    : Added db_name column.
                                 Added _date_part arg for date_part truncation. 
                                 Changed args order.
                                 Added restart_number "n"
table_skew_p2.sql              : Added. Overload of table_skew_p();
template_p.sql                 : Cosmetic updates.

2022.07.08
-------------------
catalog_storage_by_table_p.sql : Added proc.
catalog_storage_by_db.sql      : Added proc.
log_query_p.sql                : exe sec change and addl arg for splitting appname.
log_query_smry_p.sql           : Fixed AVG spill and exe secs now does includes io wait.
query_p.sql                    : exe sec added  and addl arg for splitting appname.
sysviews_grant.sql             : Added role membership output.
sysviews_p.sql                 : Added optional proc name ilike arg and fixed INT4 argument.

                            
2022.06.06
-------------------
log_query_p.sql                : Consolidate plan_sec and cmpl_secs into prep_secs.
                                 Added exe_sec.
                             
2022.04.25                   
-------------------          
column_values_p.sql            : Added proc.
db_role_privs_p.sql            : Added proc.
                             
                             
2022.04.11                   
-------------------          
Procedures:                  
log_query_pivot_p.sql          : Correct exe_sec.
log_query_smry_p.sql           : Correct exe_sec.
log_query_timing_p.sql         : Add exe_sec.
                             
                             
2022.04.09                   
-------------------          
Procedures:                  
. all_user_objs_p              : Update for db, schema, and obj ILIKE args.
. column_p()                   : Added proc.
. session_smry_p()             : No longer filters out sys_ybd_* sessions.
. session_p()                  : No longer filters out sys_ybd_* sessions.
. table_p()                    : Added proc.
. stmt_top_n_p()               : Added proc.
. version_p()                  : Added proc.
                             
                             
2022.02.24                   
-------------------          
Procedures:                  
. view_validate_p()            : Added proc.

2022.02.23
-------------------
Updated Procedures:
. log_query_pivot_p.sql        : Exe secs returns execution wall clock time.
                                 Reduced filtering of front end statements
                                 Larger set of types returned.
. log_query_smry_p.sql         : Additional metrics added.
                                 Reduced filtering of front end statements
                                 defaults to begin of previous, not current week.
Other:                        
log_query_pivot_v5.xlsx        : Renamed and updated with addl summary cols.



2021.12.09
-------------------
. Updated for inclusion in YbEasyCLI.

2021.05.09
-------------------
. Updated for only Yellowbrick version 5.x

