#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_names.py [options]
PURPOSE:
      List the table names found.
OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      The fully qualified names of all tables will be listed out, one per line.
"""
import sys
from yb_common import Util

class get_table_names(Util):
    """Issue the command used to list the table names found.
    """
    config = {
        'description': 'List/Verifies that the specified table/s exist.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'table']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in Prod --table_in sales --"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '{table_path}', 'exec_output': False}
        , 'output_tmplt_vars': ['table_path', 'schema_path', 'table', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{table_path}'
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'table':'t.name'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = ''
        dbs = [None]
        # super users get results for all DBs from sys.table
        # non-super users get results for only the connected DB from sys.table
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
        ROW_NUMBER() OVER (ORDER BY LOWER(d.name), LOWER(s.name), LOWER(t.name)) AS ordinal
        , '{{'
        || '"owner":""\" '     || NVL(u.name, '<NULL>') || ' ""\"'
        || ',"database":""\" ' || NVL(d.name, '<NULL>') || ' ""\"'
        || ',"schema":""\" '   || NVL(s.name, '<NULL>') || ' ""\"'
        || ',"table":""\" '    || NVL(t.name, '<NULL>') || ' ""\"' || '}}, ' AS data
    FROM
        sys.table AS t
        LEFT JOIN sys.schema AS s
            ON t.schema_id = s.schema_id AND t.database_id = s.database_id
        LEFT JOIN sys.database AS d
            ON t.database_id = d.database_id
        LEFT JOIN sys.user AS u
            ON t.owner_id = u.user_id
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
    gtns = get_table_names()
    
    sys.stdout.write(gtns.execute())

    exit(gtns.cmd_result.exit_code)


if __name__ == "__main__":
    main()
