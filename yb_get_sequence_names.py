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

        object_name_clause = (
            "<schema_column_name> || '.' || <object_column_name>"
            if common.args.schemas else '<object_column_name>')

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
    """ + object_name_clause + """
FROM
    o
WHERE
    %s
ORDER BY 1""" % common.filter_clause)
                     .replace('<owner_column_name>', 'o.sequenceowner')
                     .replace('<schema_column_name>', 'o.schemaname')
                     .replace('<object_column_name>', 'o.sequencename')
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
            description='List/Verifies that the specified sequence/s exist.',
            positional_args_usage='[database]',
            object_type='sequence')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group()

        common.args_process()

        return common


get_sequence_names()
