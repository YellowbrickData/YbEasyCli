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

class rstore_query_to_cstore_table:
    """Issue the ybsql command used to materialize a row store table or in
    memory query to a column store table.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize yb_rstore_query_to_cstore_table class.

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

        self.db_conn = yb_common.db_connect(self.common.args)

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_rstore_query_to_cstore_table_p'
            , args = {
                'a_query' : self.common.args.query
                , 'a_tablename' : self.common.args.table
                , 'a_create_temp_table' : ('TRUE' if self.common.args.create_temp_table else 'FALSE')
                , 'a_drop_table' : ('TRUE' if self.common.args.drop_table else 'FALSE')
                , 'a_max_varchar_size' : self.common.args.max_varchar_size}
            , pre_sql = self.common.args.pre_sql
            , post_sql = self.common.args.post_sql)

    def add_args(self):
        self.common.args_process_init(
            description=('Convert row store query to column store table.')
            , positional_args_usage='')

        self.common.args_add_optional()
        self.common.args_add_connection_group()

        args_required_grp = self.common.args_parser.add_argument_group(
            'required arguments')
        args_required_grp.add_argument(
            "--query", required=True
            , help="row store query to convert to column store table")
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name of the table that will be created")

        args_optional_grp = self.common.args_parser.add_argument_group(
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
        % rsqtocst.common.args.table)

    exit(rsqtocst.cmd_results.exit_code)


if __name__ == "__main__":
    main()