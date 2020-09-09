#!/usr/bin/env python3
"""
USAGE:
      yb_analyze_columns.py [database] table [options]

PURPOSE:
      Analyze the data content of a table's column/s.

OPTIONS:
      See the command line help message for all options.
      (yb_analyze_columns.py --help)

Output:
      Various column statistics for desired table/s column/s.
"""

import sys
import os
import re

import yb_common

class analyze_columns:
    """Issue the ybsql command used to analyze the data content of a table's column/s
    """

    def __init__(self):

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter({
            'owner':'tableowner'
            , 'schema':'schemaname'
            , 'column':'columnname'})

        sys.stdout.write('-- Running column analysis.\n')

        cmd_results = common.call_stored_proc_as_anonymous_block(
                'yb_analyze_columns_p'
                , args = {
                    'a_dbname' : common.database
                    , 'a_tablename' : common.args.table
                    , 'a_filter_clause' : filter_clause})

        cmd_results.write(tail='-- Completed column analysis.\n')

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
            description="Analyze the data content of a table's columns."
            , required_args_single=['table']
            , optional_args_multi=['owner', 'schema', 'column'])

        common.args_process()

        return common


analyze_columns()
