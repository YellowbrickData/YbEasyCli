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

    def __init__(self):

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {
                'owner':'tableowner'
                ,'schema':'schemaname'
                ,'table':'tablename'
                ,'column':'columnname'
                ,'datatype':'datatype'},
            indent='    ')

        cmd_results = common.call_stored_proc_as_anonymous_block(
                'yb_find_columns_p'
                , args = {
                    'a_column_filter_clause' : filter_clause
                }
            )

        cmd_results.write()

        exit(cmd_results.exit_code)


    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.db_args = common.db_args(
            description=
                'List column names and column attributes for filtered columns.',
            optional_args_multi=['owner', 'schema', 'table', 'column', 'datatype'],
            positional_args_usage='[database]')

        common.args_process()

        self.db_args.schema_set_all_if_none()

        return common


find_columns()
