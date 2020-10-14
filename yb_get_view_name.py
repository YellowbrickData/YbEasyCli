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

    def __init__(self, db_conn=None, db_filter_args=None):
        """Initialize get_table_name class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.db_filter_args = db_filter_args
        else:
            self.args_handler = yb_common.args_handler(
                description=
                    'List/Verifies that the specified view exists.',
                required_args_single=['view'],
                optional_args_multi=['owner'])

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
            self.db_filter_args = self.args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'},
            indent='    ')

        sql_query = """
SELECT
    --'<database_name>.' || v.schemaname || '.' || v.viewname AS view_path
    v.viewname
FROM
    {database_name}.pg_catalog.pg_views AS v
WHERE
    {filter_clause}
ORDER BY LOWER(v.schemaname), LOWER(v.viewname)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gvn = get_view_name()
    gvn.execute()

    gvn.cmd_results.write(quote=True)

    exit(gvn.cmd_results.exit_code)


if __name__ == "__main__":
    main()