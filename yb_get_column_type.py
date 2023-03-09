#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_type.py [options]
PURPOSE:
      Get a column's defined data type.
OPTIONS:
      See the command line help message for all options.
      (yb_get_column_type.py --help)
Output:
      The column's datatype is returned.
      e.g., CHARACTER(10)
            INTEGER
"""
from yb_common import Util

class get_column_type(Util):
    """Issue the ybsql command used to get a column's defined data type."""
    config = {
        'description': 'Return the data type of the requested column.'
        , 'required_args_single': ['object', 'column']
        , 'optional_args_single': ['owner', 'database', 'schema']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema dev --table sales --column price --"
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
    UPPER(pg_catalog.format_type(c.atttypid, c.atttypmod)) as datatype
FROM
    obj AS o
    LEFT JOIN sys.schema AS s
        ON o.schema_id = s.schema_id AND o.database_id = s.database_id
    LEFT JOIN sys.database AS d
        ON o.database_id = d.database_id
    LEFT JOIN sys.user AS u
        ON o.owner_id = u.user_id
    LEFT JOIN (SELECT attrelid AS object_id, attname AS name, atttypid, atttypmod FROM pg_catalog.pg_attribute) AS c
        ON o.object_id = c.object_id
WHERE
    s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND {filter_clause}
ORDER BY 1""".format(
             filter_clause = self.db_filter_sql() )

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gct = get_column_type()
    gct.execute()

    gct.cmd_results.write()

    exit(gct.cmd_results.exit_code)


if __name__ == "__main__":
    main()