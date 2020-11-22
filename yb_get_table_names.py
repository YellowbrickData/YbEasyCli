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

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
SELECT
    '{database_name}.' || c.schemaname || '.' || c.tablename AS table_path
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

        self.exec_query_and_apply_template(sql_query)

def main():
    gtns = get_table_names()
    gtns.execute()

    gtns.cmd_results.write()

    exit(gtns.cmd_results.exit_code)


if __name__ == "__main__":
    main()