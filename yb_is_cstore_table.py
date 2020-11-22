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
from yb_util import util

class is_cstore_table(util):
    """Issue the ybsql command used to determine if a table is stored as a column store table.
    """

    def init(self, db_conn=None, args_handler=None):
        """Initialize is_cstore_table class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handler
        else:
            self.args_handler = yb_common.args_handler(self.config, init_default=False)

            self.add_args()

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_is_cstore_table_p'
            , args = {
                'a_tablename' : yb_common.common.quote_object_paths(self.args_handler.args.table)})

        self.cmd_results.write()
        print(self.cmd_results.proc_return)

    def add_args(self):
        self.args_handler.args_process_init()
        self.args_handler.args_add_optional()
        self.args_handler.args_add_connection_group()
        self.args_handler.args_usage_example()

        args_required_grp = self.args_handler.args_parser.add_argument_group('required arguments')
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name ot the table to test")

def main():
    iscst = is_cstore_table(init_default=False)
    iscst.init()

    iscst.execute()

    exit(iscst.cmd_results.exit_code)


if __name__ == "__main__":
    main()