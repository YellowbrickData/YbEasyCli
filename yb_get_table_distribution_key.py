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

import yb_common
from yb_common import text


class get_table_distribution_key:
    """Issue the ybsql command used to identify the column name(s) on which
    this table is distributed.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize get_table_distribution_key class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()

            self.db_args = self.common.db_args(
                description=
                    'Identify the distribution column or type (random '
                    'or replicated) of the requested table.',
                required_args_single=['table'],
                optional_args_multi=['owner'])

            self.common.args_process()

    def exec(self):
        filter_clause = self.db_args.build_sql_filter(
            {'owner':'ownername','schema':'schemaname','table':'tablename'}
            , indent='    ')

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
             , database_name = self.common.database)

        self.cmd_results = self.common.ybsql_query(sql_query)


def main():
    gtdk = get_table_distribution_key()
    gtdk.exec()

    if gtdk.cmd_results.stdout != '':
        if gtdk.cmd_results.stdout.strip() in ('RANDOM', 'REPLICATED'):
            sys.stdout.write(gtdk.cmd_results.stdout)
        else:
            sys.stdout.write(gtdk.common.quote_object_path(gtdk.cmd_results.stdout))
    if gtdk.cmd_results.stderr != '':
        sys.stdout.write(text.color(gtdk.cmd_results.stderr, fg='red'))

    exit(gtdk.cmd_results.exit_code)


if __name__ == "__main__":
    main()