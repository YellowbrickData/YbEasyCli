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

    def __init__(self, common=None, db_args=None):
        """Initialize get_view_name class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()

            self.db_args = self.common.db_args(
                description=
                    'List/Verifies that the specified view exists.',
                required_args_single=['view'],
                optional_args_multi=['owner'])

            self.common.args_process()

    def exec(self):
        filter_clause = self.db_args.build_sql_filter(
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
             , database_name = self.common.database)

        self.cmd_results = self.common.ybsql_query(sql_query)


def main():
    gvn = get_view_name()
    gvn.exec()

    gvn.cmd_results.write(quote=True)

    exit(gvn.cmd_results.exit_code)


if __name__ == "__main__":
    main()