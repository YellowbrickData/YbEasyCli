**Table of Contents**

-  [Installation / Requirements](#installation)
-  [Overview](#overview)
-  [Contributing](#contributing)
-  [License](#license)

# YbEasyCli

An extensible collection of utilities used to interact with a Yellowbrick Data Warehouse instance through an command line interface.


<a id="installation"></a>

## Installation / Requirements

### Prerequisites

1.  **[ybtools](https://www.yellowbrick.com/docs/4.0/client_tools/client_tools_intro.html):**
    Yellowbrick client tools package
2.  **[Python](https://www.python.org)**
    - Users are encouraged to use Python 3.latest, but all utilities are currently compatible with Python 2.7 as well.
    -- to use Python 2.7 explicitly place the python command at the beginning of the command-line.
    - Executing any utility in this project as a standalone executable will default to Python 3.

### Setting Environment Variables (Optional)

All utilities rely on establishing a connection with a Yellowbrick instance.
Connection parameters are typically specified using command line flags.
Optionally, a user may save these parameters in environment variables.

See the [Yellowbrick documentation](https://www.yellowbrick.com/docs/5.2/administration/ybsql_env_variables.html) for more information about setting environment for `ybsql` connections.


<a id="overview"></a>

## Overview

### Runnable Utilities

-   **[yb_analyze_columns](./yb_analyze_columns.py):** Analyze the data content of a table's columns.
-   **[yb_check_db_views](./yb_check_db_views.py):** Check for broken views.
-   **[yb_chunk_dml_by_date_part](./yb_chunk_dml_by_date_part.py):** Chunk DML by DATE/TIMESTAMP column.
-   **[yb_chunk_dml_by_integer_yyyymmdd](./yb_chunk_dml_by_integer_yyyymmdd.py):** Chunk DML by YYYYMMDD integer column.
-   **[yb_chunk_dml_by_integer](./yb_chunk_dml_by_integer.py):** Chunk DML by INTEGER column.
-   **[yb_chunk_optimal_rows](./yb_chunk_optimal_rows.py):** Determine the optimal number of rows per chunk for a table(experimental).
-   **[yb_create_calendar_table](./yb_create_calendar_table.py):** Create a calendar dimension table.
-   **[yb_create_log_query_history](./yb_create_log_query_history.py):** Build/update long term history db table/views sourced from the sys.log_query view.
-   **[yb_ddl_sequence](./yb_ddl_sequence.py):** Return the sequence/s DDL for the requested database.  Use sequence filters to limit the set of sequences returned.
-   **[yb_ddl_stored_proc](./yb_ddl_stored_proc.py):** Return the stored procedure/s DDL for the requested database.  Use stored procedure filters to limit the set of stored procedures returned.
-   **[yb_ddl_table](./yb_ddl_table.py):** Return the table/s DDL for the requested database.  Use table filters to limit the set of tables returned.
-   **[yb_ddl_view](./yb_ddl_view.py):** Return the view/s DDL for the requested database.  Use view filters to limit the set of views returned.
-   **[yb_find_columns](./yb_find_columns.py):** List column names and column attributes for filtered columns.
-   **[yb_get_column_name](./yb_get_column_name.py):** List/Verifies that the specified table/view column name if it exists.
-   **[yb_get_column_names](./yb_get_column_names.py):** List/Verifies that the specified column names exist.
-   **[yb_get_column_type](./yb_get_column_type.py):** Return the data type of the requested column.
-   **[yb_get_sequence_names](./yb_get_sequence_names.py):** List/Verifies that the specified sequence/s exist.
-   **[yb_get_stored_proc_names](./yb_get_stored_proc_names.py):** List/Verifies that the specified stored procedures/s exist.
-   **[yb_get_table_distribution_key](./yb_get_table_distribution_key.py):** Identify the distribution column or type (random or replicated) of the requested table.
-   **[yb_get_table_name](./yb_get_table_name.py):** List/Verifies that the specified table exists.
-   **[yb_get_table_names](./yb_get_table_names.py):** List/Verifies that the specified table/s exist.
-   **[yb_get_view_name](./yb_get_view_name.py):** List/Verifies that the specified view exists.
-   **[yb_get_view_names](./yb_get_view_names.py):** List/Verifies that the specified view/s exist.
-   **[yb_is_cstore_table](./yb_is_cstore_table.py):** Determine if a table is stored as a column store table.
-   **[yb_mass_column_update](./yb_mass_column_update.py):** Update the value of multiple columns.
-   **[yb_query_to_stored_proc](./yb_query_to_stored_proc.py):** Create a stored procedure for the provided query with the query privileges of the definer/creator.
-   **[yb_sys_query_to_user_table](./yb_sys_query_to_user_table.py):** Convert system query to user table.
-   **[yb_sysprocs_all_user_objs](./yb_sysprocs_all_user_objs.py):** Report all user objects in all databases with owner and ACL details.
-   **[yb_sysprocs_column_dstr](./yb_sysprocs_column_dstr.py):** Distribution of rows per distinct values for column grouped on a logarithmic scale.
-   **[yb_sysprocs_column_stats](./yb_sysprocs_column_stats.py):** Table column metdata including estimates from statistics.
-   **[yb_sysprocs_load](./yb_sysprocs_load.py):** Transformed subset of sys.load columns for active bulk loads.
-   **[yb_sysprocs_log_bulk_xfer](./yb_sysprocs_log_bulk_xfer.py):** Transformed subset active bulk transfers (ybload & ybunload) from sys.load and sys.unload.
-   **[yb_sysprocs_log_query](./yb_sysprocs_log_query.py):** Details on completed backend statements.
-   **[yb_sysprocs_log_query_pivot](./yb_sysprocs_log_query_pivot.py):** Queries for the last week aggregated by hour for use in WLM pivot table analysis.
-   **[yb_sysprocs_log_query_smry](./yb_sysprocs_log_query_smry.py):** Aggregated subset of the sys.log_query data.
-   **[yb_sysprocs_log_query_steps](./yb_sysprocs_log_query_steps.py):** Completed statements actual vs plan metrics by plan node.
-   **[yb_sysprocs_log_query_timing](./yb_sysprocs_log_query_timing.py):** Details on completed backend statements.
-   **[yb_sysprocs_procedure](./yb_sysprocs_procedure.py):** User created stored procedures.
-   **[yb_sysprocs_query](./yb_sysprocs_query.py):** Transformed subset of sys.query columns for currently running statements.
-   **[yb_sysprocs_query_steps](./yb_sysprocs_query_steps.py):** Currently executing statements actual vs plan metrics by plan node.
-   **[yb_sysprocs_rel](./yb_sysprocs_rel.py):** All user "relations" (tables, views, & sequences) in all databases.
-   **[yb_sysprocs_rowstore](./yb_sysprocs_rowstore.py):** Rowstore overal metrics including size of data in user tables.
-   **[yb_sysprocs_rowstore_by_table](./yb_sysprocs_rowstore_by_table.py):** Size of rowstore data in user tables across all databases.
-   **[yb_sysprocs_schema](./yb_sysprocs_schema.py):** All user schemas across all databases.
-   **[yb_sysprocs_session](./yb_sysprocs_session.py):** Current session state details.
-   **[yb_sysprocs_session_smry](./yb_sysprocs_session_smry.py):** Current sessions aggregated by db, user, state, app, ip, etc...
-   **[yb_sysprocs_storage](./yb_sysprocs_storage.py):** Aggregated summary of appliance storage report.
-   **[yb_sysprocs_storage_by_db](./yb_sysprocs_storage_by_db.py):** Table storage report.
-   **[yb_sysprocs_storage_by_schema](./yb_sysprocs_storage_by_schema.py):** Storage summary by schema across one or more databases.
-   **[yb_sysprocs_storage_by_table](./yb_sysprocs_storage_by_table.py):** Table storage report.
-   **[yb_sysprocs_sysviews](./yb_sysprocs_sysviews.py):** Names and arguments for all installed sysviews procedures.
-   **[yb_sysprocs_table_constraints](./yb_sysprocs_table_constraints.py):** Existing constraints on user tables as per information_schema.table_constraints.
-   **[yb_sysprocs_table_skew](./yb_sysprocs_table_skew.py):** Table skew report.
-   **[yb_sysprocs_wlm_active_profile](./yb_sysprocs_wlm_active_profile.py):** Returns current active WLM profile configuration details by pool.
-   **[yb_sysprocs_wlm_active_rule](./yb_sysprocs_wlm_active_rule.py):** Current active WLM profile rules.
-   **[yb_sysprocs_wlm_state](./yb_sysprocs_wlm_state.py):** Returns current active WLM profile state metrics by pool.
-   **[yb_to_yb_copy_table](./yb_to_yb_copy_table.py):** Copy a table from a source cluster to a destination cluster.
-   **[yb_wl_profiler_heatmap](./yb_wl_profiler_heatmap.py):** Creates a 35 day Excel heatmap of Work Loads on a Yellowbrick Cluster.

### Other Files

-   **[yb_util](./yb_util.py):** Parent class for all utilities
-   **[yb_common](./yb_common.py):** Performs functions such as argument parsing, login verification, logging,
    and command execution that are common to all utilities in this project.
-   **[yb_ddl_object](./yb_ddl_object.py):** Dump out the SQL/DDL that was used to create any database object.
    - This file is typically not executed directly, but it is relied upon by:
      1.  [yb_ddl_sequence](./yb_ddl_sequence.py)
      2.  [yb_ddl_table](./yb_ddl_table.py)
      3.  [yb_ddl_view](./yb_ddl_view.py)

-   **[test_create_host_objects](./test/test_create_host_objects.py):** Create test user, database, and database objects.
-   **[test_drop_host_objects](./test/test_drop_host_objects.py):** Drop test user, database, and database objects.
-   **[test_run](./test/test_run.py):** Runs the test created for all utilities or a given utility.


<a id="contributing"></a>

## Contributing

Contributors will be expected to sign the Contributors License Agreement
associated with this project. A bot will evaluate whether the CLA has been
signed when you create a pull request. If necessary, the bot will leave a comment
prompting you to accept the agreement.

Before creating a pull request, ensure that changes are properly tested. See the
[guide](./test/README.md) for running and developing tests for more information.

<a id="license"></a>

## License

YbEasyCli is distributed under the [MIT License](./LICENSE). Using and modifying
these utilities should be done at your own risk.
