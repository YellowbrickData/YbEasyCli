#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_name.py [options]
PURPOSE:
      List/Verifies that the specified column exists in the object.
OPTIONS:
      See the command line help message for all options.
      (yb_get_column_name.py --help)
Output:
      If the column exists in the object, it's fully qualified name will be echoed back out.
"""
from yb_common import Util

class get_column_name(Util):
    """Issue the ybsql command used to verify that the specified column exists.
    """
    config = {
        'description': 'List/Verifies that the specified table/view column name if it exists.'
        , 'required_args_single': ['object', 'column']
        , 'optional_args_single': ['owner', 'database', 'schema', ]
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema dev --object sales --column price --"
            , 'file_args': [Util.conn_args_file] }
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'object':'o.name', 'column':'c.name'} }


    def execute(self):
        sql_query = ''
        if not(self.args_handler.args.database):
            self.args_handler.args.database = self.db_conn.database
        # the query will be using pg_catalog.pg_attribute, which is not a cross-database table,
        #   therefore it must be run connected to the database being searched
        elif self.args_handler.args.database != self.db_conn.database:
            sql_query = '\\c %s' % self.args_handler.args.database
        if not(self.args_handler.args.schema):
            self.args_handler.args.schema = self.db_conn.schema

        sql_query += """
WITH
obj AS (
    SELECT view_id AS object_id, schema_id, database_id, owner_id, name, 'v' AS otype FROM sys.VIEW
    UNION ALL SELECT table_id AS object_id, schema_id, database_id, owner_id, name, 't' AS otype FROM sys.table
)
SELECT
    d.name || '.' || s.name || '.' || o.name || '.' || c.name
FROM
    obj AS o
    LEFT JOIN sys.schema AS s
        ON o.schema_id = s.schema_id AND o.database_id = s.database_id
    LEFT JOIN sys.database AS d
        ON o.database_id = d.database_id
    LEFT JOIN sys.user AS u
        ON o.owner_id = u.user_id
    LEFT JOIN (SELECT attrelid AS object_id, attname AS name FROM pg_catalog.pg_attribute) AS c
        ON o.object_id = c.object_id
WHERE
    s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND {filter_clause}
ORDER BY 1""".format(
             filter_clause = self.db_filter_sql() )

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gcn = get_column_name()
    gcn.execute()

    gcn.cmd_results.write(quote=True)

    exit(gcn.cmd_results.exit_code)


if __name__ == "__main__":
    main()