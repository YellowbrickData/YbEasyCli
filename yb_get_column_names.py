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
from yb_common import Util

class get_column_names(Util):
    """Issue the ybsql command used to list the column names comprising an
    object.
    """
    config = {
        'description': 'List/Verifies that the specified column names exist.'
        , 'required_args_single': ['object']
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'column']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema dev -- sales"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['column_path', 'object_path', 'schema_path', 'column', 'table', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{column}'
        , 'db_filter_args': {'owner':'owner', 'schema':'schema', 'object':'object', 'column':'clmn'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = """
WITH
objct AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(n.nspname), LOWER(c.relname), a.attnum) AS ordinal
        , a.attname AS clmn
        , a.attnum AS columnnum
        , c.relname AS object
        , n.nspname AS schema
        , pg_get_userbyid(c.relowner) AS owner
    FROM {database}.pg_catalog.pg_class AS c
        LEFT JOIN {database}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        schema NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND object = '{object}'
        AND c.relkind IN ('r', 'v')
        AND a.attnum > 0
        AND {filter_clause}
)
SELECT
    DECODE(ordinal, 1, '', ', ')
    || '{{' || '"ordinal": ' || ordinal::VARCHAR || ''
    || ',"owner":""\" '    || owner        || ' ""\"'
    || ',"database":""\" ' || '{database}' || ' ""\"'
    || ',"schema":""\" '   || schema       || ' ""\"'
    || ',"object":""\" '   || object       || ' ""\"'
    || ',"column":""\" '   || clmn         || ' ""\"' || '}}' AS data
FROM objct
ORDER BY ordinal""".format(
             filter_clause = self.db_filter_sql()
             , database    = self.db_conn.database
             , object      = self.args_handler.args.object)

        return self.exec_query_and_apply_template(sql_query, exec_output=self.args_handler.args.exec_output)

def main():
    gcns = get_column_names()
    
    print(gcns.execute())

if __name__ == "__main__":
    main()