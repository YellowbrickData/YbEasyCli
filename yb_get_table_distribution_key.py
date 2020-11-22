#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_distribution_key.py [database] [options]

PURPOSE:
      Identify the column name(s) on which this table is distributed.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_distribution_key.py --help)

Output:
      The columns comprising the distribution key are echoed out, one column
      name per line, in the order in which they were specified in the
      DISTRIBUTE ON ( )   clause.

      If the table is distributed on random (round-robin), then this script will
      simply return the string  RANDOM
"""
import sys

from yb_common import common
from yb_util import util

class get_table_distribution_key(util):
    """Issue the ybsql command used to identify the column name(s) on which
    this table is distributed.
    """

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
WITH
tbl AS (
    SELECT
        DECODE(
            LOWER(t.distribution)
                , 'hash', t.distribution_key
                , UPPER(t.distribution)
        ) AS distribution
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS ownername
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        LEFT JOIN {database_name}.sys.table AS t
            ON c.oid = t.table_id
    WHERE
        c.relkind = 'r'::CHAR
)
SELECT
    distribution
FROM
    tbl
WHERE
    distribution IS NOT NULL
    AND {filter_clause}""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

        if self.cmd_results.stdout != '':
            if self.cmd_results.stdout.strip() in ('RANDOM', 'REPLICATED'):
                sys.stdout.write(self.cmd_results.stdout)
            else:
                sys.stdout.write(common.quote_object_paths(self.cmd_results.stdout))
        if self.cmd_results.stderr != '':
            sys.stdout.write(text.color(self.cmd_results.stderr, fg='red'))

def main():
    gtdk = get_table_distribution_key()
    gtdk.execute()

    exit(gtdk.cmd_results.exit_code)

if __name__ == "__main__":
    main()