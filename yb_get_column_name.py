#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_name.py [database] object column [options]

PURPOSE:
      List/Verifies that the specified column exists in the object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_name.py --help)

Output:
      If the column exists in the object, it's name will be echoed back out.
"""

import sys

import yb_common


class get_column_name:
    """Issue the ybsql command used to verify that the specified column
    exists.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize get_column_name class.

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
                    'List/Verifies that the specified table/view column'
                    'name if it exists.',
                required_args_single=['object', 'column'],
                optional_args_multi=['owner'])

            self.common.args_process()

        self.db_conn = yb_common.db_connect(self.common.args)

    def execute(self):
        filter_clause = self.db_args.build_sql_filter(
            {'owner':'objectowner'
            , 'schema':'schemaname'
            , 'object':'objectname'
            , 'column':'columnname'})

        sql_query = """
WITH
objct AS (
    SELECT
        a.attname AS columnname
        , c.relname AS objectname
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS objectowner
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database_name}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN ('r', 'v')
)
SELECT
    --'<database_name>.' || schemaname || '.' || objectname || '.' || columnname AS column_path
    columnname
FROM
    objct
WHERE
    {filter_clause}
ORDER BY LOWER(schemaname), LOWER(objectname)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gcn = get_column_name()
    gcn.execute()

    gcn.cmd_results.write(quote=True)

    exit(gcn.cmd_results.exit_code)


if __name__ == "__main__":
    main()