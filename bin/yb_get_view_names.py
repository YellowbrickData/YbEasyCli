#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_names.py [options]
PURPOSE:
      List the view names.
OPTIONS:
      See the command line help message for all options.
      (yb_get_view_names.py --help)

Output:
      The fully qualified names of all views will be listed out, one per line.
"""
import sys
from yb_common import Util

class get_view_names(Util):
    """Issue the ybsql command used to list the view names.
    """
    config = {
        'description': 'List/Verifies that the specified view/s exist.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --schema_in dev Prod --'
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['view_path', 'schema_path', 'view', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{view_path}'
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'view':'v.name'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()
 
        sql_query = ''
        dbs = [None]
        # super users get results for all DBs from sys.view
        # non-super users get results for only the connected DB from sys.view
        #    for non-super users a db array is created to individually connect and run query 
        if not(self.db_conn.ybdb['is_super_user']):
            dbs = self.get_dbs()

        for db in dbs:
            if not(self.db_conn.ybdb['is_super_user']):
                sql_query += '\\c %s' % db

            sql_query += """
WITH
data as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(d.name), LOWER(s.name), LOWER(v.name)) AS ordinal
        , '{{'
        || '"owner":""\" '     || NVL(u.name, '<NULL>') || ' ""\"'
        || ',"database":""\" ' || NVL(d.name, '<NULL>') || ' ""\"'
        || ',"schema":""\" '   || NVL(s.name, '<NULL>') || ' ""\"'
        || ',"view":""\" '     || NVL(v.name, '<NULL>') || ' ""\"' || '}}, ' AS data
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
        AND {filter_clause}
)
SELECT data FROM data ORDER BY ordinal;\n""".format(
                filter_clause = self.db_filter_sql() )

        self.cmd_result = self.db_conn.ybsql_query(sql_query)
        self.cmd_result.on_error_exit()

        data = ''
        ordinal = 1
        for line in self.cmd_result.stdout.splitlines():
            data += line.replace('{', '{"ordinal":""\" %d ""\", ' % ordinal) + '\n'
            ordinal += 1

        return self.apply_template(data, exec_output=self.args_handler.args.exec_output)

def main():
    gvns = get_view_names()

    sys.stdout.write(gvns.execute())

    exit(gvns.cmd_result.exit_code)


if __name__ == "__main__":
    main()