**Table of Contents**

-  [Installation / Requirements](#installation)
-  [Overview](#overview)
-  [Contributing](#contributing)
-  [License](#license)

# YbEasyCli

An extensible collection of utilities used to interact with a Yellowbrick Data Warehouse instance through an easy command line interface.


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

See the [Yellowbrick documentation](https://www.yellowbrick.com/docs/4.0/administration/ybsql_connections.html#reference_qtb_5ft_sv__ybsql_connections_environment_variables) for more information about setting environment for `ybsql` connections.


<a id="overview"></a>

## Overview

### Runnable Utilities

-   **[yb\_analyze\_columns](./yb_analyze_columns.py):** Analyze the data content of a table's columns.
-   **[yb_check_db_views](./yb_check_db_views.py):** Check for broken views.
-   **[yb_chunk_dml_by_date_part](./yb_chunk_dml_by_date_part.py):** Chunk DML by DATE/TIMESTAMP column.
-   **[yb_chunk_dml_by_integer](./yb_chunk_dml_by_integer.py):** Chunk DML by INTEGER column.
-   **[yb_chunk_dml_by_yyyymmdd_integer](./yb_chunk_dml_by_yyyymmdd_integer.py):** Chunk DML by YYYYMMDD integer column.
-   **[yb_chunk_optimal_rows](./yb_chunk_optimal_rows.py):** Determine the optimal number of rows per chunk for a table(experimental).
-   **[yb_ddl_sequence](./yb_ddl_sequence.py):** Return the sequence/s DDL for the requested database.  Use sequence filters to limit the set of tables returned.
-   **[yb_ddl_table](./yb_ddl_table.py):** Return the table/s DDL for the requested database.  Use table filters to limit the set of tables returned.
-   **[yb_ddl_view](./yb_ddl_view.py):** Return the view/s DDL for the requested database.  Use view filters to limit the set of tables returned.
-   **[yb_find_columns](./yb_find_columns.py):** List column names and column attributes for filtered columns.
-   **[yb\_get\_column\_name](./yb_get_column_name.py):** List/Verifies that the specified table/view column name if it exists.
-   **[yb\_get\_column\_names](./yb_get_column_names.py):** List/Verifies that the specified column names exist.
-   **[yb\_get\_column\_type](./yb_get_column_type.py):** Return the data type of the requested column.
-   **[yb\_get\_sequence\_names](./yb_get_sequence_names.py):** List/Verifies that the specified sequence/s exist.
-   **[yb\_get\_table\_distribution\_key](./yb_get_table_distribution_key.py):** Identify the distribution column or type (random or replicated) of the requested table.
-   **[yb\_get\_table\_name](./yb_get_table_name.py):** List/Verifies that the specified table exists.
-   **[yb\_get\_table\_names](./yb_get_table_names.py):** List/Verifies that the specified table/s exist.
-   **[yb\_get\_view\_name](./yb_get_view_name.py):** List/Verifies that the specified view exists.
-   **[yb\_get\_view\_names](./yb_get_view_names.py):** List/Verifies that the specified view/s exist.
-   **[yb\_is\_cstore\_table](./yb_is_cstore_table.py):** Determine if a table is stored as a column store table.
-   **[yb\_mass\_column\_update](./yb_mass_column_update.py):** Update the value of multiple columns.
-   **[yb\_query\_to\_stored\_proc](./yb_query_to_stored_proc.py):** Create a stored procedure for the provided query with the query privileges of the definer/creator.
-   **[yb\_rstore\_query\_to\_cstore\_table](./yb_rstore_query_to_cstore_table.py):** Convert row store query to column store table.
-   **[yb\_to\_yb\_copy\_table](./yb_to_yb_copy_table.py):** Copy a table from a source cluster to a destination cluster.

### Other Files

-   **[yb\_util](./yb_util.py):** Parent class for all utilities
-   **[yb\_common](./yb_common.py):** Performs functions such as argument parsing, login verification, logging,
    and command execution that are common to all utilities in this project.
-   **[yb\_ddl\_object](./yb_ddl_object.py):** Dump out the SQL/DDL that was used to create any database object.
    - This file is typically not executed directly, but it is relied upon by:
      1.  [yb\_ddl\_sequence](./yb_ddl_sequence.py)
      2.  [yb\_ddl\_table](./yb_ddl_table.py)
      3.  [yb\_ddl\_view](./yb_ddl_view.py)

-   **[test\_create\_host\_objects](./test/test_create_host_objects.py):** Create test user, database, and database objects.
-   **[test\_drop\_host\_objects](./test/test_drop_host_objects.py):** Drop test user, database, and database objects.
-   **[test\_run](./test/test_run.py):** Runs the test created for all utilities or a given utility.


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
