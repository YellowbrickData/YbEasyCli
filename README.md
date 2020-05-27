**Table of Contents**

-  [Installation / Requirements](#installation)
-  [Overview](#overview)
-  [Contributing](#contributing)
-  [License](#license)

# YbEasyCli

An extensible collection of utility scripts used to interact with a Yellowbrick Data Warehouse instance through an easy command line interface.

<a id="installation"></a>

## Installation / Requirements

### Prerequisites

1.  **[ybtools](https://www.yellowbrick.com/docs/4.0/client_tools/client_tools_intro.html):**
    Yellowbrick client tools package
2.  **[Python](https://www.python.org)**
    - Users are encouraged to use Python 3.latest, but all scripts are currently
      compatible with Python 2.7 as well.
    - Executing any script in this project as a standalone executable will
      default to Python 3. 

### Setting Environment Variables (Optional)

All utilities rely on establishing a connection with a Yellowbrick instance.
Connection parameters are typically specified using command line flags.
Optionally, a user may save these parameters in environment variables.

See the [Yellowbrick
documentation](https://www.yellowbrick.com/docs/4.0/administration/ybsql_connections.html#reference_qtb_5ft_sv__ybsql_connections_environment_variables)
for more information about setting environment for `ybsql` connections.


<a id="overview"></a>

## Overview

### Runnable Scripts

-   **[yb\_ddl\_table](./yb_ddl_table.py):** Dump out the SQL/DDL that was used
    to create a table.
-   **[yb\_get\_column\_name](./yb_get_column_name.py):** Verifies that the
    specified column exists in the object.
-   **[yb\_get\_column\_names](./yb_get_column_names.py):** List the column
    names comprising a database object.
-   **[yb\_get\_column\_type](./yb_get_column_type.py):** Get a column's defined
    data type.
-   **[yb\_get\_table\_distribution\_key](./yb_get_table_distribution_key.py):**
    Identify the column name(s) on which this table is distributed.
-   **[yb\_get\_table\_name](./yb_get_table_name.py):** Verifies that the
    specified table exists.
-   **[yb\_get\_table\_names](./yb_get_table_names.py):** List the table names
    found in this database.
-   **[yb\_get\_view\_name](./yb_get_view_name.py):** Verifies that the
    specified view exists.
-   **[yb\_get\_view\_names](./yb_get_view_names.py):** List the view names
    found in this database.
-   **[yb\_get\_seqeuence\_names](./yb_get_sequence_names.py):** List the
    sequence names found in this database.
-   **[yb\_ddl\_view](./yb_ddl_view.py):** Dump out the SQL/DDL that was
    used to create a view.
-   **[yb\_ddl\_sequence](./yb_ddl_sequence.py):** Dump out the SQL/DDL that
    was used to create a sequence. 

### Other Files

-   **[yb\_ddl\_object](./yb_ddl_object.py):** Dump out the SQL/DDL that was
    used to create any database object.
    - This file is typically not executed directly, but it is relied upon by:
      1.  [yb\_ddl\_sequence](./yb_ddl_sequence.py)
      2.  [yb\_ddl\_table](./yb_ddl_table.py)
      3.  [yb\_ddl\_view](./yb_ddl_view.py)
-   **[yb\_common](./yb_common.py):** Performs functions such as argument
    parsing, login verification, logging, and command execution that are common
    to all utilities in this project.

-   **[test\_run](./test/test_run.py):** Runs the test created for a given utility
    script.
-   **[test\_run\_all](./test/test_run_all_utils.sh):** Runs all tests.


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
these utility scripts should be done at your own risk. 
