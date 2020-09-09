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


class get_table_distribution_key:
    """Issue the ybsql command used to identify the column name(s) on which
    this table is distributed.
    """

    def __init__(self):

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'ownername','schema':'schemaname','table':'tablename'},
            indent='    ')

        sql_query = (("""
WITH
t AS (
    SELECT
        DECODE(
            LOWER(t.distribution)
                , 'hash', t.distribution_key
                , UPPER(t.distribution)
        ) AS distribution
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS ownername
    FROM <database>.pg_catalog.pg_class AS c
        LEFT JOIN <database>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        LEFT JOIN <database>.sys.table AS t
            ON c.oid = t.table_id
    WHERE
        c.relkind = 'r'::CHAR
)
SELECT
    t.distribution
FROM
    t
WHERE
    t.distribution IS NOT NULL
    AND <filter_clause>""")
            .replace('<filter_clause>', filter_clause)
            .replace('<database>', common.database))

        cmd_results = common.ybsql_query(sql_query)

        if cmd_results.stdout != '':
            if cmd_results.stdout.strip() in ('RANDOM', 'REPLICATED'):
                sys.stdout.write(cmd_results.stdout)
            else:
                sys.stdout.write(common.quote_object_path(cmd_results.stdout))
        if cmd_results.stderr != '':
            sys.stdout.write(common.color(cmd_results.stderr, fg='red'))

        exit(cmd_results.exit_code)

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.db_args = common.db_args(
            description=
                'Identify the distribution column or type (random '
                'or replicated) of the requested table.',
            required_args_single=['table'],
            optional_args_multi=['owner'])

        common.args_process()

        return common


get_table_distribution_key()
