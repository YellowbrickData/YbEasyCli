#!/usr/bin/env python3

"""
USAGE:
      yb_chunk_dml_by_integer.py [options]

PURPOSE:
      Create/execute DML chunked by an integer column.

OPTIONS:
      See the command line help message for all options.
      (yb_chunk_dml_by_integer.py --help)

Output:
      Chunked DML statements.
"""
import sys

import yb_common
from yb_util import util

class chunk_dml_by_integer(util):
    """Issue the ybsql command used to create/execute DML chunked by an integer column
    """

    def init(self, db_conn=None, args_handler=None):
        """Initialize chunk_dml_by_integer class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handler
        else:
            self.args_handler = yb_common.args_handler(self.config, init_default=False)

            self.add_args()

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)

        if '<chunk_where_clause>' not in self.args_handler.args.dml:
            yb_common.common.error("DML must contain the string '<chunk_where_clause>'")

        if not self.args_handler.args.execute_chunk_dml:
            self.args_handler.args.pre_sql = ''
            self.args_handler.args.post_sql = ''

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_chunk_dml_by_integer_p'
            , args = {
                'a_table_name' : self.args_handler.args.table
                , 'a_integer_column_name' : self.args_handler.args.column
                , 'a_dml' : self.args_handler.args.dml
                , 'a_table_where_clause' : self.args_handler.args.table_where_clause
                , 'a_min_chunk_size' : self.args_handler.args.chunk_rows
                , 'a_verbose' : ('TRUE' if self.args_handler.args.verbose_chunk_off else 'FALSE')
                , 'a_add_null_chunk' : ('TRUE' if self.args_handler.args.null_chunk_off else 'FALSE')
                , 'a_print_chunk_dml' : ('TRUE' if self.args_handler.args.print_chunk_dml else 'FALSE')
                , 'a_execute_chunk_dml' : ('TRUE' if self.args_handler.args.execute_chunk_dml else 'FALSE')}
            , pre_sql = self.args_handler.args.pre_sql
            , post_sql = self.args_handler.args.post_sql)

    def add_args(self):
        self.args_handler.args_process_init()
        self.args_handler.args_add_optional()
        self.args_handler.args_add_connection_group()
        self.args_handler.args_usage_example()

        args_chunk_r_grp = self.args_handler.args_parser.add_argument_group(
            'required chunking arguments')
        args_chunk_r_grp.add_argument(
            "--table", required=True
            , help="table name, the name may be qualified if needed")
        args_chunk_r_grp.add_argument(
            "--column", required=True
            , help="the column which is used to create chunks on the"
                " DML, the column must be a integer data type")
        args_chunk_r_grp.add_argument(
            "--dml", required=True
            , help="DML to perform  in chunks, the DML"
                " must contain the string '<chunk_where_clause>' to properly facilitate the"
                " dynamic chunking filter")
        args_chunk_r_grp.add_argument(
            "--chunk_rows", dest="chunk_rows", required=True
            , type=yb_common.intRange(1,9223372036854775807)
            , help="the minimum rows that each chunk should contain")

        args_chunk_o_grp = self.args_handler.args_parser.add_argument_group(
            'optional chunking arguments')
        args_chunk_o_grp.add_argument("--table_where_clause", default="TRUE"
            , help="filter the records to chunk, if this filter is applied it should also be"
                " part of dml provided")
        args_chunk_o_grp.add_argument("--verbose_chunk_off", action="store_false"
            , help="don't print additional chunking details, defaults to FALSE")
        args_chunk_o_grp.add_argument("--null_chunk_off", action="store_false"
            , help="don't create a chunk where the chunking column is NULL, defaults to FALSE")
        args_chunk_o_grp.add_argument("--print_chunk_dml", action="store_true"
            , help="print the chunked DML, defaults to FALSE")
        args_chunk_o_grp.add_argument("--execute_chunk_dml", action="store_true"
            , help="execute the chunked DML, defaults to FALSE")
        args_chunk_o_grp.add_argument("--pre_sql", default=''
            , help="SQL to run before the chunking DML, only runs if execute_chunk_dml is set")
        args_chunk_o_grp.add_argument("--post_sql", default=''
            , help="SQL to run after the chunking DML, only runs if execute_chunk_dml is set")


def main():
    cdml = chunk_dml_by_integer(init_default=False)
    cdml.init()

    sys.stdout.write('-- Running DML chunking.\n')
    cdml.execute()
    cdml.cmd_results.write(tail='-- Completed DML chunking.\n')

    exit(cdml.cmd_results.exit_code)


if __name__ == "__main__":
    main()