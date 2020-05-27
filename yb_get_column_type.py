#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_type.py [database] table column [options]

PURPOSE:
      Get a column's defined data type.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_type.py --help)

Output:
      The column's datatype is returned.
      e.g., CHARACTER(10)
            INTEGER
"""
import sys

import yb_common


class get_column_type:
    """Issue the ybsql command used to get a column's defined data type."""

    def __init__(self):

        common = self.init_common()

        sql_query = (("""
WITH
dt AS (
    SELECT
        UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as datatype
        , a.attname AS columnname
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM <database_name>.pg_catalog.pg_class AS c
        LEFT JOIN <database_name>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <database_name>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind = 'r'::CHAR
)
SELECT
    datatype
FROM
    dt
WHERE
    <object_column_name> = '%s'
    AND <table_column_name> = '%s'
    AND %s""" % (common.args.column[0],
                 common.args.table[0],
                 common.filter_clause))
                     .replace('<owner_column_name>', 'dt.tableowner')
                     .replace('<object_column_name>', 'dt.columnname')
                     .replace('<table_column_name>', 'dt.tablename')
                     .replace('<schema_column_name>', 'dt.schemaname')
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
            description='Return the data type of the requested column.',
            positional_args_usage='[database] table column',
            object_type='object')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group(keep_args=['--owner', '--schema', '--in'])

        common.args_process()

        return common


get_column_type()
