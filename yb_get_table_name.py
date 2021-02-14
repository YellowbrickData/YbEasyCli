#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_name.py [options]

PURPOSE:
      Verifies that the specified table exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_name.py --help)

Outputs:
      If the table exists, it's fully qualified name will be echoed back out.
"""
from yb_common import Util

class get_table_name(Util):
    """Issue the command used to verify that the specified table exists."""
    config = {
        'description': 'List/Verifies that the specified table exists.'
        , 'required_args_single': ['table']
        , 'optional_args_single': ['owner', 'database', 'schema']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --current_schema dev --table sales --'
            , 'file_args': [Util.conn_args_file] }
        , 'db_filter_args': {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'} }

    def execute(self):
        sql_query = """
SELECT
    --'<database_name>.' || c.schemaname || '.' || c.tablename AS table_path
    c.tablename
FROM
    {database}.information_schema.tables AS t
    JOIN {database}.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND {filter_clause}
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""".format(
             filter_clause   = self.db_filter_sql()
             , database      = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

def main():
    gtn = get_table_name()
    gtn.execute()

    gtn.cmd_results.write(quote=True)

    exit(gtn.cmd_results.exit_code)


if __name__ == "__main__":
    main()