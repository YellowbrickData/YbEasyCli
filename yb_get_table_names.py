#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_names.py [database] [options]

PURPOSE:
      List the table names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      The fully qualified names of all tables will be listed out, one per line.
"""
from yb_util import util

class get_table_names(util):
    """Issue the command used to list the table names found in a particular
    database.
    """
    config = {
        'description': 'List/Verifies that the specified table/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'table']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in Prod --table_in sales --"
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '{table_path}', 'exec_output': False}
        , 'output_tmplt_vars': ['table_path', 'schema_path', 'table', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{table_path}'
        , 'db_filter_args': {'owner':'c.tableowner', 'schema':'c.schemaname', 'table':'c.tablename'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = """
WITH
data as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(c.schemaname), LOWER(c.tablename)) AS ordinal
        , DECODE(ordinal, 1, '', ', ')
        || '{{' || '"ordinal": ' || ordinal::VARCHAR || ''
        || ',"owner":""\" '    || c.tableowner || ' ""\"'
        || ',"database":""\" ' || '{database}' || ' ""\"'
        || ',"schema":""\" '   || c.schemaname || ' ""\"'
        || ',"table":""\" '    || c.tablename  || ' ""\"' || '}}' AS data
    FROM
        {database}.information_schema.tables AS t
        JOIN {database}.pg_catalog.pg_tables AS c
            ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
    WHERE
        c.schemaname NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND t.table_type='BASE TABLE'
        AND {filter_clause}
)
SELECT data FROM data ORDER BY ordinal""".format(
             filter_clause = self.db_filter_sql()
             , database = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)
        return self.exec_query_and_apply_template(sql_query)

def main():
    gtns = get_table_names()
    
    print(gtns.execute())

    exit(gtns.cmd_results.exit_code)


if __name__ == "__main__":
    main()