#!/usr/bin/env python3

"""
USAGE:
      yb_chunk_optimal_rows.py [options]

PURPOSE:
      Determine the optimal number of rows per chunk for a table.

OPTIONS:
      See the command line help message for all options.
      (yb_chunk_optimal_rows.py --help)

OUTPUT:
      Number of rows to use when performing chunked DML on a table.

NOTES:
      This calculation is experimental.
"""

import sys

import yb_common

class chunk_dml_by_date_part:
    """Issue the ybsql command to determine the optimal number of rows per chunk for a table.
    """

    def __init__(self):

        common = self.init_common()

        schema = (
            common.args.schema
            if common.args.schema
            else common.schema)

        db = (
            common.args.db
            if common.args.db
            else common.database)

        cmd_results = common.call_stored_proc_as_anonymous_block(
            'yb_chunk_optimal_rows_p'
            , args = {
                'a_table_name' : common.args.table
                , 'a_schema_name' : schema
                , 'a_db_name' : db})

        if cmd_results.stderr != '' and cmd_results.exit_code == 0:
            sys.stdout.write(cmd_results.proc_return)

        print(cmd_results.proc_return)

        exit(
            cmd_results.exit_code
            if cmd_results.proc_return is not None or cmd_results.exit_code != 0
            else 1)

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.db_args = common.db_args(
            description='Determine the optimal number of rows per chunk for a table.'
            , required_args_single=['table']
            , optional_args_single=['db', 'schema']
            , positional_args_usage=[])

        common.args_process()

        return common

chunk_dml_by_date_part()