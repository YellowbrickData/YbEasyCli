#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_name.py [database] object column [options]

PURPOSE:
      List/Verifies that the specified column exists in the object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_name.py --help)

Output:
      If the column exists in the object, it's name will be echoed back out.
"""
from yb_util import util

class get_column_name(util):
    """Issue the ybsql command used to verify that the specified column
    exists.
    """
    config = {
        'description': 'List/Verifies that the specified table/view column name if it exists.'
        , 'required_args_single': ['object', 'column']
        , 'optional_args_single': ['owner', 'database', 'schema', ]
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema dev --object sales --column price --"
            , 'file_args': [util.conn_args_file] }
        , 'db_filter_args': {'owner':'objectowner', 'schema':'schemaname', 'object':'objectname', 'column':'columnname'} }


    def execute(self):
        sql_query = """
WITH
objct AS (
    SELECT
        a.attname AS columnname
        , c.relname AS objectname
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS objectowner
    FROM {database}.pg_catalog.pg_class AS c
        LEFT JOIN {database}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN ('r', 'v')
)
SELECT
    columnname
FROM
    objct
WHERE
    {filter_clause}
ORDER BY LOWER(schemaname), LOWER(objectname)""".format(
             filter_clause = self.db_filter_sql()
             , database = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gcn = get_column_name()
    gcn.execute()

    gcn.cmd_results.write(quote=True)

    exit(gcn.cmd_results.exit_code)


if __name__ == "__main__":
    main()