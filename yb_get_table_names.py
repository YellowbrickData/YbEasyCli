#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_names.py [database] [options]

PURPOSE:
      List the table names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      The fully qualified names of all tables will be listed out, one per line.
"""

import sys

import yb_common


class get_table_names:
    """Issue the command used to list the table names found in a particular
    database.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize get_table_names class.

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
                    'List/Verifies that the specified table/s exist.'
                , optional_args_multi=['owner', 'schema', 'table'])

            self.common.args_process()

    def exec(self):
        filter_clause = self.db_args.build_sql_filter(
            {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'}
            , indent='    ')

        sql_query = """
SELECT
    '{database_name}.' || c.schemaname || '.' || c.tablename AS table_path
FROM
    {database_name}.information_schema.tables AS t
    JOIN {database_name}.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND {filter_clause}
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""".format(
             filter_clause = filter_clause
             , database_name = self.common.database)

        self.cmd_results = self.common.ybsql_query(sql_query)


def main():
    gtns = get_table_names()
    gtns.exec()

    gtns.cmd_results.write(quote=True)

    exit(gtns.cmd_results.exit_code)


if __name__ == "__main__":
    main()