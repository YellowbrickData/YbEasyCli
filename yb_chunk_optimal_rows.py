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

class chunk_optimal_rows:
    """Issue the ybsql command to determine the optimal number of rows per chunk for a table.
    """

    def __init__(self, db_conn=None, args_handler=None):
        """Initialize chunk_optimal_rows class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handler
        else:
            self.args_handler = yb_common.args_handler(
                description='Determine the optimal number of rows per chunk for a table.'
                , required_args_single=['table']
                , optional_args_single=['database', 'schema']
                , positional_args_usage=[])

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)

    def execute(self):
        schema = (
            self.args_handler.args.schema
            if self.args_handler.args.schema
            else self.db_conn.schema)

        database = (
            self.args_handler.args.database
            if self.args_handler.args.database
            else self.db_conn.database)

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_chunk_optimal_rows_p'
            , args = {
                'a_table_name' : self.args_handler.args.table
                , 'a_schema_name' : schema
                , 'a_db_name' : database})

def main():
    cors = chunk_optimal_rows()
    cors.execute()

    if cors.cmd_results.stderr != '' and cors.cmd_results.exit_code == 0:
        sys.stdout.write(cors.cmd_results.proc_return)

    print(cors.cmd_results.proc_return)

    exit(
        cors.cmd_results.exit_code
        if cors.cmd_results.proc_return is not None or cors.cmd_results.exit_code != 0
        else 1)

if __name__ == "__main__":
    main()