#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_names.py [database] [options]

PURPOSE:
      List the view names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_view_names.py --help)

Output:
      The fully qualified names of all views will be listed out, one per line.
"""

import sys

import yb_common


class get_view_names:
    """Issue the ybsql command used to list the view names found in a particular
    database.
    """

    def __init__(self):
        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'},
            indent='    ')

        sql_query = (("""
SELECT
    '<database_name>.' || v.schemaname || '.' || v.viewname AS view_path
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
                'List/Verifies that the specified view/s exist.',
            optional_args_multi=['owner', 'schema', 'view'])

        common.args_process()

        return common


get_view_names()
