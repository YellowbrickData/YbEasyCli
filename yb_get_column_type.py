#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_type.py [database] [options]

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

    def __init__(self, db_conn=None, db_filter_args=None):
        """Initialize get_column_type class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.db_filter_args = db_filter_args
        else:
            args_handler = yb_common.args_handler(
                description='Return the data type of the requested column.',
                required_args_single=['table', 'column'],
                optional_args_multi=['owner'])

            args_handler.args_process()
            self.db_conn = yb_common.db_connect(args_handler.args)
            self.db_filter_args = args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter({
            'owner':'tableowner'
            , 'schema':'schemaname'
            , 'table':'tablename'
            , 'column':'columnname'})

        sql_query = """
WITH
dt AS (
    SELECT
        UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as datatype
        , a.attname AS columnname
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database_name}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind = 'r'::CHAR
)
SELECT
    datatype
FROM
    dt
WHERE
    {filter_clause}""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gct = get_column_type()
    gct.execute()

    gct.cmd_results.write()

    exit(gct.cmd_results.exit_code)


if __name__ == "__main__":
    main()