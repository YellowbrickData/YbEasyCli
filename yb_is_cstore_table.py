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

class yb_is_cstore_table:
    """Issue the ybsql command used to determine if a table is stored as a column store table.
    """

    def __init__(self):

        common = self.init_common()

        cmd_results = common.call_stored_proc_as_anonymous_block(
            'yb_is_cstore_table_p'
            , args = {
                'a_tablename' : common.args.table})

        cmd_results.write()
        print(cmd_results.proc_return)

        exit(cmd_results.exit_code)

    def add_args(self, common):
        common.args_process_init(
            description=('Determine if a table is stored as a column store table.')
            , positional_args_usage='')

        common.args_add_optional()
        common.args_add_connection_group()

        args_required_grp = common.args_parser.add_argument_group('required arguments')
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name ot the table to test")

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.add_args(common)

        common.args_process()

        return common


yb_is_cstore_table()