#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_name.py [database] table [options]

PURPOSE:
      Verifies that the specified table exists.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_name.py --help)

Outputs:
      If the table exists, it's fully qualified name will be echoed back out.
"""

import sys

import yb_common


class get_table_name:
    """Issue the command used to verify that the specified table exists."""

    def __init__(self, db_conn=None, db_filter_args=None):
        """Initialize get_table_name class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.db_filter_args = db_filter_args
        else:
            args_handler = yb_common.args_handler(
                description=
                    'List/Verifies that the specified table exists.'
                , required_args_single=['table']
                , optional_args_multi=['owner'])

            args_handler.args_process()
            self.db_conn = yb_common.db_connect(args_handler.args)
            self.db_filter_args = args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'}
            , indent='    ')

        sql_query = """
SELECT
    --'<database_name>.' || c.schemaname || '.' || c.tablename AS table_path
    c.tablename
FROM
    {database_name}.information_schema.tables AS t
    JOIN {database_name}.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND {filter_clause}
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gtn = get_table_name()
    gtn.execute()

    gtn.cmd_results.write(quote=True)

    exit(gtn.cmd_results.exit_code)


if __name__ == "__main__":
    main()