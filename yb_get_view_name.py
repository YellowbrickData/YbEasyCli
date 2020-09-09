#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_name.py [database] view [options]

PURPOSE:
      Verifies that the specified view exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_view_name.py --help)

Output:
      If the view exists, it's fully qualified name will be echoed back out.
"""

import sys

import yb_common


class get_view_name:
    """Issue the ybsql command used to verify that the specified view exists."""

    def __init__(self):
        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'},
            indent='    ')

        sql_query = (("""
SELECT
    --'<database_name>.' || v.schemaname || '.' || v.viewname AS view_path
    v.viewname
FROM
    <database_name>.pg_catalog.pg_views AS v
WHERE
    <filter_clause>
ORDER BY LOWER(v.schemaname), LOWER(v.viewname)""")
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
                'List/Verifies that the specified view exists.',
            required_args_single=['view'],
            optional_args_multi=['owner'])

        common.args_process()

        return common


get_view_name()
