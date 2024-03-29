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

from yb_common import StoredProc, Util

class chunk_optimal_rows(Util):
    """Issue the ybsql command to determine the optimal number of rows per chunk for a table.
    """
    config = {
        'description': 'Determine the optimal number of rows per chunk for a table.'
        , 'required_args_single': ['table']
        , 'optional_args_single': ['database', 'schema']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --table dze_db1.dev.sales --schema dev'
            , 'file_args': [Util.conn_args_file] } }

    def execute(self):
        schema = (
            self.args_handler.args.schema
            if self.args_handler.args.schema
            else self.db_conn.schema)

        database = (
            self.args_handler.args.database
            if self.args_handler.args.database
            else self.db_conn.database)

        self.cmd_results = StoredProc('yb_chunk_optimal_rows_p', self.db_conn).call_proc_as_anonymous_block(
            args = {
                'a_table'      : self.args_handler.args.table
                , 'a_schema'   : schema
                , 'a_database' : database})

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