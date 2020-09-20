#!/usr/bin/env python3
"""
USAGE:
      yb_is_cstore_table.py [options]

PURPOSE:
      Determine if a table is stored as a column store table

OPTIONS:
      See the command line help message for all options.
      (yb_is_cstore_table.py --help)

Output:
      True/False
"""

import sys
import os
import re

import yb_common

class is_cstore_table:
    """Issue the ybsql command used to determine if a table is stored as a column store table.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize is_cstore_table class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()

            self.add_args()

            self.common.args_process()

    def exec(self):
        self.cmd_results = self.common.call_stored_proc_as_anonymous_block(
            'yb_is_cstore_table_p'
            , args = {
                'a_tablename' : self.common.args.table})

    def add_args(self):
        self.common.args_process_init(
            description=('Determine if a table is stored as a column store table.')
            , positional_args_usage='')

        self.common.args_add_optional()
        self.common.args_add_connection_group()

        args_required_grp = self.common.args_parser.add_argument_group('required arguments')
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name ot the table to test")


def main():
    iscst = is_cstore_table()

    iscst.exec()
    iscst.cmd_results.write()
    print(iscst.cmd_results.proc_return)

    exit(iscst.cmd_results.exit_code)


if __name__ == "__main__":
    main()