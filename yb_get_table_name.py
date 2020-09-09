#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_name.py [database] table [options]

PURPOSE:
      Verifies that the specified table exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_name.py --help)

Outputs:
      If the table exists, it's fully qualified name will be echoed back out.
"""

import sys

import yb_common


class get_table_name:
    """Issue the command used to verify that the specified table exists."""

    def __init__(self):
        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'},
            indent='    ')

        sql_query = (("""
SELECT
    --'<database_name>.' || c.schemaname || '.' || c.tablename AS table_path
    c.tablename
FROM
    <database_name>.information_schema.tables AS t
    JOIN <database_name>.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND <filter_clause>
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""")
            .replace('<filter_clause>', filter_clause)
            .replace('<database_name>', common.database))

        cmd_results = common.ybsql_query(sql_query)

        cmd_results.write(quote=True)

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
            description=
                'List/Verifies that the specified table exists.',
            required_args_single=['table'],
            optional_args_multi=['owner'])

        common.args_process()

        return common


get_table_name()
