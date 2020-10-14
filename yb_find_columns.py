#!/usr/bin/env python3
"""
USAGE:
      yb_find_columns.py [database] [options]

PURPOSE:
      List all columns found for the provided filter.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The column names and column attributes for filtered columns.
"""

import sys

import yb_common


class find_columns:
    """Issue the ybsql command used to list the column names comprising an
    object.
    """

    def __init__(self, db_conn=None, db_filter_args=None):
        """Initialize find_columns class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if db_conn:
            self.db_conn = db_conn
            self.db_filter_args = db_filter_args
        else:
            args_handler = yb_common.args_handler(
                description=
                    'List column names and column attributes for filtered columns.',
                optional_args_multi=['owner', 'schema', 'table', 'column', 'datatype'],
                positional_args_usage='[database]')

            args_handler.args_process()
            self.db_conn = yb_common.db_connect(args_handler.args)
            self.db_filter_args = args_handler.db_filter_args

        self.db_filter_args.schema_set_all_if_none()

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {
                'owner':'tableowner'
                ,'schema':'schemaname'
                ,'table':'tablename'
                ,'column':'columnname'
                ,'datatype':'datatype'}
            , indent='    ')

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_find_columns_p'
                , args = {
                    'a_column_filter_clause' : filter_clause
                }
            )


def main():
    fcs = find_columns()
    fcs.execute()

    fcs.cmd_results.write()

    exit(fcs.cmd_results.exit_code)


if __name__ == "__main__":
    main()