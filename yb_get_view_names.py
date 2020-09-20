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

    def __init__(self, common=None, db_args=None):
        """Initialize get_view_names class.

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
                    'List/Verifies that the specified view/s exist.',
                optional_args_multi=['owner', 'schema', 'view'])

            self.common.args_process()

    def exec(self):
        filter_clause = self.db_args.build_sql_filter(
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
             , database_name = self.common.database)

        self.cmd_results = self.common.ybsql_query(sql_query)


def main():
    gvns = get_view_names()
    gvns.exec()

    gvns.cmd_results.write(quote=True)

    exit(gvns.cmd_results.exit_code)


if __name__ == "__main__":
    main()