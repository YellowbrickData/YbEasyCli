#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_names.py [options]

PURPOSE:
      List the column names comprising an object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_names.py --help)

Output:
      The column names for the object will be listed out, one per line.
"""
import sys
from yb_common import Util

class get_column_names(Util):
    """Issue the ybsql command used to list the column names comprising an object.
    """
    config = {
        'description': 'List/Verifies that the specified column names exist.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'object', 'column']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema dev -- sales"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['column_path', 'object_path', 'schema_path', 'column', 'object', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{column_path}'
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'object':'o.name', 'column':'c.name'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = ''
        for db in self.get_dbs():
            if not(self.db_conn.ybdb['is_super_user']):
                sql_query += '\\c %s' % db

            sql_query += """
WITH
obj AS (
    SELECT view_id AS object_id, schema_id, database_id, owner_id, name, 'v' AS otype FROM sys.VIEW
    UNION ALL SELECT table_id AS object_id, schema_id, database_id, owner_id, name, 't' AS otype FROM sys.table
)
, data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(d.name), LOWER(s.name), LOWER(o.name), object_ordinal) AS ordinal
        , '{{'
        || '"owner":""\" '     || NVL(u.name, '<NULL>') || ' ""\"'
        || ',"database":""\" ' || NVL(d.name, '<NULL>') || ' ""\"'
        || ',"schema":""\" '   || NVL(s.name, '<NULL>') || ' ""\"'
        || ',"object":""\" '   || NVL(o.name, '<NULL>') || ' ""\"'
        || ',"column":""\" '   || NVL(c.name, '<NULL>') || ' ""\"' || '}}, ' AS data
    FROM
        obj AS o
        LEFT JOIN sys.schema AS s
            ON o.schema_id = s.schema_id AND o.database_id = s.database_id
        LEFT JOIN sys.database AS d
            ON o.database_id = d.database_id
        LEFT JOIN sys.user AS u
            ON o.owner_id = u.user_id
        LEFT JOIN (SELECT attrelid AS object_id, attname AS name, attnum AS object_ordinal FROM pg_catalog.pg_attribute) AS c
            ON o.object_id = c.object_id
    WHERE
        s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND c.object_ordinal > 0
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

        return self.exec_query_and_apply_template(sql_query, exec_output=self.args_handler.args.exec_output)

def main():
    gcns = get_column_names()
    
    sys.stdout.write(gcns.execute())

    exit(gcns.cmd_result.exit_code)

if __name__ == "__main__":
    main()