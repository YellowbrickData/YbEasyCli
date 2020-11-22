#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_name.py [database] table [options]

PURPOSE:
      Verifies that the specified table exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_name.py --help)

Outputs:
      If the table exists, it's fully qualified name will be echoed back out.
"""
from yb_util import util

class get_table_name(util):
    """Issue the command used to verify that the specified table exists."""

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
SELECT
    --'<database_name>.' || c.schemaname || '.' || c.tablename AS table_path
    c.tablename
FROM
    {database_name}.information_schema.tables AS t
    JOIN {database_name}.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND {filter_clause}
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

def main():
    gtn = get_table_name()
    gtn.execute()

    gtn.cmd_results.write(quote=True)

    exit(gtn.cmd_results.exit_code)


if __name__ == "__main__":
    main()