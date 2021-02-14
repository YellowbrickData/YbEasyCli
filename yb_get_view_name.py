#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_name.py [options]

PURPOSE:
      Verifies that the specified view exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_view_name.py --help)

Output:
      If the view exists, it's fully qualified name will be echoed back out.
"""
from yb_common import Util

class get_view_name(Util):
    """Issue the ybsql command used to verify that the specified view exists."""
    config = {
        'description': 'List/Verifies that the specified view exists.'
        , 'required_args_single': ['view']
        , 'optional_args_single': ['owner', 'database', 'schema']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --schema Prod --view sales_v --'
            , 'file_args': [Util.conn_args_file] }
        , 'db_filter_args': {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'} }

    def execute(self):
        sql_query = """
SELECT
    --'<database_name>.' || v.schemaname || '.' || v.viewname AS view_path
    v.viewname
FROM
    {database_name}.pg_catalog.pg_views AS v
WHERE
    {filter_clause}
ORDER BY LOWER(v.schemaname), LOWER(v.viewname)""".format(
             filter_clause = self.db_filter_sql()
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

def main():
    gvn = get_view_name()
    gvn.execute()

    gvn.cmd_results.write(quote=True)

    exit(gvn.cmd_results.exit_code)

if __name__ == "__main__":
    main()