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
    config = {
        'description': 'Determine if a table is stored as a column store table.'
        , 'required_args_single': ['table']
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --table sys.blade --'
            , 'file_args': [util.conn_args_file] } }

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_is_cstore_table_p'
            , args = {
                'a_table' : yb_common.common.quote_object_paths(self.args_handler.args.table)})

        self.cmd_results.write()
        print(self.cmd_results.proc_return)

def main():
    iscst = is_cstore_table()

    iscst.execute()

    exit(iscst.cmd_results.exit_code)


if __name__ == "__main__":
    main()