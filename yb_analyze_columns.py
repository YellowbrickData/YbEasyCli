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

    def __init__(self, db_conn=None, args_handler=None, db_filter_args=None):
        """Initialize analyze_columns class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handler
            self.db_filter_args = db_filter_args
        else:
            self.args_handler = yb_common.args_handler(
                description="Analyze the data content of a table's columns."
                , required_args_single=['table']
                , optional_args_multi=['owner', 'schema', 'column'])

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
            self.db_filter_args = self.args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter({
            'owner':'tableowner'
            , 'schema':'schemaname'
            , 'column':'columnname'})

        sys.stdout.write('-- Running column analysis.\n')

        cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_analyze_columns_p'
                , args = {
                    'a_dbname' : self.db_conn.database
                    , 'a_tablename' : self.args_handler.args.table
                    , 'a_filter_clause' : filter_clause})

        cmd_results.write(tail='-- Completed column analysis.\n')

        exit(cmd_results.exit_code)


def main():
    acs = analyze_columns()
    acs.execute()

if __name__ == "__main__":
    main()