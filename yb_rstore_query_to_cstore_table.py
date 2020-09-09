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

class yb_rstore_query_to_cstore_table:
    """Issue the ybsql command used to materialize a row store table or in
    memory query to a column store table.
    """

    def __init__(self):

        common = self.init_common()

        sys.stdout.write('-- Converting row store query to column store table.\n')

        cmd_results = common.call_stored_proc_as_anonymous_block(
            'yb_rstore_query_to_cstore_table_p'
            , args = {
                'a_query' : common.args.query
                , 'a_tablename' : common.args.table
                , 'a_create_temp_table' : ('TRUE' if common.args.create_temp_table else 'FALSE')
                , 'a_drop_table' : ('TRUE' if common.args.drop_table else 'FALSE')
                , 'a_max_varchar_size' : common.args.max_varchar_size}
            , pre_sql = common.args.pre_sql
            , post_sql = common.args.post_sql)

        cmd_results.write(tail='-- The %s column store table has been created.\n'
            % common.args.table)

        exit(cmd_results.exit_code)


    def add_args(self, common):
        common.args_process_init(
            description=('Convert row store query to column store table.')
            , positional_args_usage='')

        common.args_add_optional()
        common.args_add_connection_group()

        args_required_grp = common.args_parser.add_argument_group('required arguments')
        args_required_grp.add_argument(
            "--query", required=True
            , help="row store query to convert to column store table")
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name of the table that will be created")

        args_optional_grp = common.args_parser.add_argument_group('optional arguments')
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


yb_rstore_query_to_cstore_table()
