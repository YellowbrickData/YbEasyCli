#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_type.py [database] [options]

PURPOSE:
      Get a column's defined data type.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_type.py --help)

Output:
      The column's datatype is returned.
      e.g., CHARACTER(10)
            INTEGER
"""
import sys

import yb_common


class get_column_type:
    """Issue the ybsql command used to get a column's defined data type."""

    def __init__(self):

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'tableowner',
            'schema':'schemaname',
            'table':'tablename',
            'column':'columnname'})

        sql_query = (("""
WITH
dt AS (
    SELECT
        UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as datatype
        , a.attname AS columnname
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM <database_name>.pg_catalog.pg_class AS c
        LEFT JOIN <database_name>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <database_name>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind = 'r'::CHAR
)
SELECT
    datatype
FROM
    dt
WHERE
    <filter_clause>""")
            .replace('<filter_clause>', filter_clause)
            .replace('<database_name>', common.database))

        cmd_results = common.ybsql_query(sql_query)

        cmd_results.write()

        exit(cmd_results.exit_code)

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.db_args = common.db_args(
            description='Return the data type of the requested column.',
            required_args_single=['table', 'column'],
            optional_args_multi=['owner'])

        common.args_process()

        return common


get_column_type()
