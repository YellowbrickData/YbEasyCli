README.TXT

Yellowbrick Data Technical Support sysviews version 5 README file.
Revision 2021.05.09

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

      procedure       |                                              description
----------------------+-------------------------------------------------------------------------------------------------------
 all_user_objs_p      | All user objects in all databases with owner and ACL detail.
 analyze_immed_sess_p | SETs ybd_analyze_after_writes TO [OFF|ON]` for session as a superuser.
 analyze_immed_user_p | Run ALTER USER session_user SET ybd_analyze_after_writes as a superuser.
 bulk_xfer_p          | Transformed subset active bulk transfers (ybload & ybunload) from sys.load and sys.unload.
 column_dstr_p        | Distribution of rows per distinct values for column grouped on a logarithmic scale
 column_stats_p       | Table column metadata including cardinality estimates from the db statistics.
 help_p               | Returns usage information on sys *_p procedures.
 log_bulk_xfer_p      | Transformed subset completed bulk transfers (ybload & ybunload) from sys.log_load and sys.log_unload.
 log_query_p          | Details on completed backend statements.
 log_query_pivot_p    | Queries for the last week aggregated by hour for use in WLM pivot table analysis.
 log_query_smry_p     | Aggregated subset of the sys.log_query data.
 log_query_steps_p    | Completed statements actual vs plan metrics by plan node.
 log_query_timing_p   | Details on completed backend statements.
 procedure_p          | User created stored procedures.
 query_p              | Transformed subset of sys.query columns for currently running statements.
 query_steps_p        | Currently executing statements actual vs plan metrics by plan node.
 rel_p                | All user "relations" (tables, views, & sequences) in all databases.
 rowstore_by_table_p  | Size of rowstore data in user tables across all databases.\r
 rowstore_p           | Rowstore overal metrics including size of data in user tables.\r
 schema_p             | All user schemas across all databases.
 session_p            | Current session state details.
 session_smry_p       | Current sessions aggregated by db, user, state, app, ip, etc...
 storage_by_db_p      | Storage space of committed blocks in user tables aggregated by database.
 storage_by_schema_p  | Storage summary by schema across one or more databases.
 storage_by_table_p   | Storage summary for user tables by database, schema, and table.
 storage_p            | Aggregated appliance storage for data, spill, other, and total space.
 sysviews_p           | Names and arguments for all installed sysviews procedures.
 table_constraints_p  | Existing constraints on user tables as per information_schema.table_constraints.
 wlm_active_profile_p | Returns current active WLM profile configuration details by pool.
 wlm_active_rule_p    | Current active WLM profile rules.
 wlm_state_p          | Returns current active WLM profile state metrics by pool.
(31 rows)
  
  
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

2021.12.09
-------------------
Updated for inclusion in YbEasyCLI.

2021.05.09
-------------------
. Updated for only Yellowbrick version 5.x

  