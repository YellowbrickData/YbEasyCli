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
    config = {
        'description': 'List/Verifies that the specified view/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'view']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --schema_in dev Prod --'
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['view_path', 'schema_path', 'view', 'schema', 'database']
        , 'output_tmplt_default': '<view_path>'
        , 'db_filter_args': {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
SELECT
    '{database_name}.' || v.schemaname || '.' || v.viewname AS view_path
FROM
    {database_name}.pg_catalog.pg_views AS v
WHERE
    v.schemaname NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND {filter_clause}
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