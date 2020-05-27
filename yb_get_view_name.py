#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_name [database] view [options]

PURPOSE:
      Verifies that the specified view exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_view_name.py --help)

Output:
      If the view exists, its name will be echoed back out.
"""

import sys

import yb_common


class get_view_name:
    """Issue the ybsql command used to verify that the specified view exists."""

    def __init__(self):
        common = self.init_common()

        object_name_clause = (
            "<schema_column_name> || '.' || <object_column_name>"
            if common.args.schemas else '<object_column_name>')

        sql_query = (("""
SELECT
    """ + object_name_clause + """
FROM
    <database_name>.pg_catalog.pg_views AS v
WHERE
    <object_column_name> = '""" + common.args.view[0] + """'
    AND %s""" % common.filter_clause)
                     .replace('<owner_column_name>', 'v.viewowner')
                     .replace('<schema_column_name>', 'v.schemaname')
                     .replace('<object_column_name>', 'v.viewname')
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
            description='List/Verifies that the specified view exists.',
            positional_args_usage='[database] view',
            object_type='view')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group(keep_args=['--owner', '--schema', '--in'])

        common.args_process()

        return common


get_view_name()
