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
                    'List/Verifies that the specified view/s exist.',
                optional_args_multi=['owner', 'schema', 'view'])

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
            self.db_filter_args = self.args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'}
            , indent='    ')

        sql_query = """
SELECT
    '{database_name}.' || v.schemaname || '.' || v.viewname AS view_path
FROM
    {database_name}.pg_catalog.pg_views AS v
WHERE
    {filter_clause}
ORDER BY LOWER(v.schemaname), LOWER(v.viewname)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gvns = get_view_names()
    gvns.execute()

    gvns.cmd_results.write(quote=True)

    exit(gvns.cmd_results.exit_code)


if __name__ == "__main__":
    main()