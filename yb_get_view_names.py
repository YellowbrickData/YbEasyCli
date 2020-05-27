#!/usr/bin/env python3
"""
USAGE:
      yb_get_view_names [database] [options]

PURPOSE:
      List the view names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_view_names.py --help)

Output:
      The names of all views will be listed out, one per line.
"""

import sys

import yb_common


class get_view_names:
    """Issue the ybsql command used to list the view names found in a particular
    database.
    """

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
    %s
ORDER BY 1""" % common.filter_clause)
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
            description='List/Verifies that the specified view/s exist.',
            positional_args_usage='[database]',
            object_type='view')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group()

        common.args_process()

        return common


get_view_names()
