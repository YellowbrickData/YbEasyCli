#!/usr/bin/env python3
"""
USAGE:
      yb_rstore_query_to_cstore_table.py [options]

PURPOSE:
      Materialize a row store table or in memory query to a column store table.

OPTIONS:
      See the command line help message for all options.
      (yb_rstore_query_to_cstore_table.py --help)

Output:
      A column store table.
"""

import sys
import os
import re

import yb_common
from yb_util import util

class rstore_query_to_cstore_table(util):
    """Issue the ybsql command used to materialize a row store table or in
    memory query to a column store table.
    """

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_rstore_query_to_cstore_table_p'
            , args = {
                'a_query' : self.args_handler.args.query
                , 'a_tablename' : self.args_handler.args.table
                , 'a_create_temp_table' : ('TRUE' if self.args_handler.args.create_temp_table else 'FALSE')
                , 'a_drop_table' : ('TRUE' if self.args_handler.args.drop_table else 'FALSE')
                , 'a_max_varchar_size' : self.args_handler.args.max_varchar_size}
            , pre_sql = self.args_handler.args.pre_sql
            , post_sql = self.args_handler.args.post_sql)

    def additional_args(self):
        args_required_grp = self.args_handler.args_parser.add_argument_group(
            'required arguments')
        args_required_grp.add_argument(
            "--query", required=True
            , help="row store query to convert to column store table")
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name of the table that will be created")

        args_optional_grp = self.args_handler.args_parser.add_argument_group(
            'optional arguments')
        args_optional_grp.add_argument("--create_temp_table", action="store_true"
            , help="create destination table as temporary table, defaults to FALSE")
        args_optional_grp.add_argument("--drop_table", action="store_true"
            , help="first drop the destination table if it exists, defaults to FALSE")
        args_optional_grp.add_argument("--max_varchar_size"
            , type=yb_common.intRange(1,64000), default=10000
            , help="truncate size of all VARCHAR columns in the destination table, defaults to 10000")
        args_optional_grp.add_argument("--pre_sql", default=''
            , help="SQL to run before the creation of the destination table")
        args_optional_grp.add_argument("--post_sql", default=''
            , help="SQL to run after the creation of the destination table")

def main():
    rsqtocst = rstore_query_to_cstore_table()

    sys.stdout.write('-- Converting row store query to column store table.\n')
    rsqtocst.execute()
    rsqtocst.cmd_results.write(tail='-- The %s column store table has been created.\n'
        % rsqtocst.args_handler.args.table)

    exit(rsqtocst.cmd_results.exit_code)


if __name__ == "__main__":
    main()