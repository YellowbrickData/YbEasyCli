#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_name [database] table [options]

PURPOSE:
      Verifies that the specified table exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_name.py --help)

Outputs:
      If the table exists, its name will be echoed back out.
"""

import sys

import yb_common


class get_table_name:
    """Issue the command used to verify that the specified table exists."""

    def __init__(self):

        common = self.init_common()

        object_name_clause = (
            "<schema_column_name> || '.' || <object_column_name>"
            if common.args.schemas else '<object_column_name>')

        sql_query = (("""
SELECT
    """ + object_name_clause + """
FROM
    <database_name>.pg_catalog.pg_tables AS t
WHERE
    <object_column_name> = '""" + common.args.table[0] + """'
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
            description='Verifies that the specified table exists.',
            positional_args_usage='[database] table',
            object_type='table')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group(keep_args=['--owner', '--schema', '--in'])

        common.args_process()

        return common


get_table_name()
