#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_distribution_key.py [database] table [options]

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

        sql_query = (("""
WITH
t AS (
    SELECT
        DECODE(LOWER(t.distribution), 'hash', t.distribution_key,
        UPPER(t.distribution)) AS distribution
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM <database_name>.pg_catalog.pg_class AS c
        LEFT JOIN <database_name>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        LEFT JOIN <database_name>.sys.table AS t
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
    AND <object_column_name> = '""" + common.args.table[0] + """'
    AND %s""" % common.filter_clause)
                     .replace('<owner_column_name>', 't.tableowner')
                     .replace('<schema_column_name>', 't.schemaname')
                     .replace('<object_column_name>', 't.tablename')
                     .replace('<database_name>', common.database))

        cmd_results = common.ybsql_query(sql_query)

        if cmd_results.exit_code == 0:
            sys.stdout.write(cmd_results.stdout)
        else:
            sys.stdout.write(common.color(cmd_results.stderr, fg='red'))
            exit(cmd_results.exit_code)

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common(
            description=('Identify the distribution column or type (random '
                         'or replicated) of the requested table.'),
            positional_args_usage='[database] table',
            object_type='table')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group(keep_args=['--owner', '--schema', '--in'])

        common.args_process()

        return common


get_table_distribution_key()
