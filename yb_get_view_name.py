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
      If the view exists, it's name will be echoed back out.
"""
from yb_common import Util

class get_view_name(Util):
    """Issue the command used to verify that the specified view exists."""
    config = {
        'description': 'List/Verifies that the specified view exists.'
        , 'required_args_single': ['view']
        , 'optional_args_single': ['owner', 'database', 'schema']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --schema Prod --view sales_v --'
            , 'file_args': [Util.conn_args_file] }
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'view':'v.name'} }

    def execute(self):
        sql_query = ''
        if not(self.db_conn.ybdb['is_super_user']) and self.args_handler.args.database:
            sql_query = '\\c %s' % self.args_handler.args.database
        if not(self.args_handler.args.database):
            self.args_handler.args.database = self.db_conn.database
        if not(self.args_handler.args.schema):
            self.args_handler.args.schema = self.db_conn.schema

        sql_query += """
SELECT
    v.name
FROM
    sys.view AS v
    LEFT JOIN sys.schema AS s
        ON v.schema_id = s.schema_id AND v.database_id = s.database_id
    LEFT JOIN sys.database AS d
        ON v.database_id = d.database_id
    LEFT JOIN sys.user AS u
        ON v.owner_id = u.user_id
WHERE
    s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND {filter_clause}""".format(
             filter_clause = self.db_filter_sql() )

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

def main():
    gvn = get_view_name()
    gvn.execute()

    gvn.cmd_results.write(quote=True)

    exit(gvn.cmd_results.exit_code)


if __name__ == "__main__":
    main()