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
from yb_util import util

class get_view_names(util):
    """Issue the ybsql command used to list the view names found in a particular
    database.
    """

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

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

        self.exec_query_and_apply_template(sql_query)

def main():
    gvns = get_view_names()
    gvns.execute()

    gvns.cmd_results.write()

    exit(gvns.cmd_results.exit_code)


if __name__ == "__main__":
    main()