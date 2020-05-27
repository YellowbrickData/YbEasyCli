#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_names.py [database] object [options]

PURPOSE:
      List the column names comprising an object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_names.py --help)

Output:
      The column names for the object will be listed out, one per line.
"""

import sys

import yb_common


class get_column_names:
    """Issue the ybsql command used to list the column names comprising an
    object.
    """

    def __init__(self):

        common = self.init_common()

        sql_query = (("""
WITH
o AS (
    SELECT
        a.attname AS columnname
        , a.attnum AS columnnum
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM <database_name>.pg_catalog.pg_class AS c
        LEFT JOIN <database_name>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <database_name>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN ('r', 'v')
        AND a.attnum > 0
)
SELECT
    columnname
FROM
    o
WHERE
    <table_column_name> = '%s'
    AND %s
ORDER BY
    columnnum""" % (common.args.object[0],
                    common.filter_clause))
                     .replace('<owner_column_name>', 'o.tableowner')
                     .replace('<object_column_name>', 'o.columnname')
                     .replace('<table_column_name>', 'o.tablename')
                     .replace('<schema_column_name>', 'o.schemaname')
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
            description='List/Verifies that the specified column names exist.',
            positional_args_usage='[database] object',
            object_type='object')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group()

        common.args_process()

        return common


get_column_names()
