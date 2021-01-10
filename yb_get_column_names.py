#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_names.py [database] object [options]

PURPOSE:
      List the column names comprising an object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_names.py --help)

Output:
      The column names for the object will be listed out, one per line.
"""
from yb_util import util

class get_column_names(util):
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
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['table_path', 'schema_path', 'column', 'table', 'schema', 'database']
        , 'output_tmplt_default': '<column>'
        , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'object':'objectname', 'column':'columnname'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
WITH
objct AS (
    SELECT
        a.attname AS columnname
        , a.attnum AS columnnum
        , c.relname AS objectname
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database_name}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN ('r', 'v')
        AND a.attnum > 0
)
SELECT
    '{database_name}.' || schemaname || '.' || objectname || '.' || columnname AS column_path
FROM
    objct
WHERE
    schemaname NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND objectname = '{object_name}'
    AND {filter_clause}
ORDER BY
    LOWER(schemaname), LOWER(objectname), columnnum""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database
             , object_name = self.args_handler.args.object)

        self.exec_query_and_apply_template(sql_query, quote_default=True)

def main():
    gcns = get_column_names()
    gcns.execute()

    gcns.cmd_results.write()

    exit(gcns.cmd_results.exit_code)


if __name__ == "__main__":
    main()