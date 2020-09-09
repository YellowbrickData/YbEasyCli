#!/usr/bin/env python3
"""
USAGE:
      yb_get_sequence_names.py [database] [options]

PURPOSE:
      List the sequence names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_sequence_names.py --help)

Output:
      The names of all sequences will be listed out, one per line.
"""

import sys

import yb_common


class get_sequence_names:
    """Issue the ybsql command to list the sequences found in a particular
    database.
    """

    def __init__(self):

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'sequenceowner',
            'schema':'schemaname',
            'sequence':'sequencename'})

        sql_query = (("""
WITH
o AS (
    SELECT
        c.relname AS sequencename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS sequenceowner
    FROM <database_name>.pg_catalog.pg_class AS c
        LEFT JOIN <database_name>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
    WHERE
        c.relkind IN ('S')
)
SELECT
    schemaname || '.' || sequencename
FROM
    o
WHERE
    <filter_clause>
ORDER BY LOWER(schemaname), LOWER(sequencename)""")
            .replace('<filter_clause>', filter_clause)
            .replace('<database_name>', common.database))

        cmd_results = common.ybsql_query(sql_query)

        cmd_results.write(quote=True)

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
                'List/Verifies that the specified sequence/s exist.',
            optional_args_multi=['sequence', 'owner', 'schema'])

        common.args_process()

        return common


get_sequence_names()
