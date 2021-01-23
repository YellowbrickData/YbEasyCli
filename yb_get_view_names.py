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
        , 'output_tmplt_vars': ['view_path', 'schema_path', 'view', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{view_path}'
        , 'db_filter_args': {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()
 
        sql_query = """
WITH
data as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(v.schemaname), LOWER(v.viewname)) AS ordinal
        , DECODE(ordinal, 1, '', ', ')
        || '{{' || '"ordinal": ' || ordinal::VARCHAR || ''
        || ',"owner":""\" '    || v.viewowner  || ' ""\"'
        || ',"database":""\" ' || '{database}' || ' ""\"'
        || ',"schema":""\" '   || v.schemaname || ' ""\"'
        || ',"view":""\" '     || v.viewname   || ' ""\"' || '}}' AS data
    FROM
        {database}.pg_catalog.pg_views AS v
    WHERE
        v.schemaname NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND {filter_clause}
)
SELECT data FROM data ORDER BY ordinal""".format(
             filter_clause = self.db_filter_sql()
             , database = self.db_conn.database)

        return self.exec_query_and_apply_template(sql_query)

def main():
    gvns = get_view_names()

    print(gvns.execute())

    exit(gvns.cmd_results.exit_code)


if __name__ == "__main__":
    main()