#!/usr/bin/env python3

"""
USAGE:
      yb_chunk_dml_by_yyyymmdd_integer.py [options]

PURPOSE:
      Create/execute DML chunked by an yyyymmdd integer column.

OPTIONS:
      See the command line help message for all options.
      (yb_chunk_dml_by_yyyymmdd_integer.py --help)

Output:
      Chunked DML statements.
"""

import sys

import yb_common

class chunk_dml_by_yyyymmdd_integer:
    """Issue the ybsql command used to create/execute DML chunked by an yyyymmdd integer column
    """

    def __init__(self, common=None, db_args=None):
        """Initialize chunk_dml_by_yyyymmdd_integer class.

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

        if '<chunk_where_clause>' not in self.common.args.dml:
            sys.stderr.write("DML must contain the string '<chunk_where_clause>'\n")
            exit(1)

        if not self.common.args.execute_chunk_dml:
            self.common.args.pre_sql = ''
            self.common.args.post_sql = ''

    def exec(self):
        self.cmd_results = self.common.call_stored_proc_as_anonymous_block(
            'yb_chunk_dml_by_yyyymmdd_integer_p'
            , args = {
                'a_table_name' : self.common.args.table
                , 'a_yyyymmdd_column_name' : self.common.args.column
                , 'a_dml' : self.common.args.dml
                , 'a_min_chunk_size' : self.common.args.chunk_rows
                , 'a_verbose' : ('TRUE' if self.common.args.verbose_chunk_off else 'FALSE')
                , 'a_add_null_chunk' : ('TRUE' if self.common.args.null_chunk_off else 'FALSE')
                , 'a_print_chunk_dml' : ('TRUE' if self.common.args.print_chunk_dml else 'FALSE')
                , 'a_execute_chunk_dml' : ('TRUE' if self.common.args.execute_chunk_dml else 'FALSE')}
            , pre_sql = self.common.args.pre_sql
            , post_sql = self.common.args.post_sql)

    def add_args(self):
        self.common.args_process_init(
            description=('Chunk DML by YYYYMMDD integer column.')
            , positional_args_usage='')

        self.common.args_add_optional()
        self.common.args_add_connection_group()

        args_chunk_r_grp = self.common.args_parser.add_argument_group(
            'chunking required arguments')
        args_chunk_r_grp.add_argument(
            "--table", required=True
            , help="table name, the name may be qualified if needed")
        args_chunk_r_grp.add_argument(
            "--dml", required=True
            , help="DML to perform  in chunks, the DML"
                " must contain the string '<chunk_where_clause>' to properly facilitate the"
                " dynamic chunking filter")
        args_chunk_r_grp.add_argument(
            "--column", required=True
            , help="the column which is used to create chunks on the"
                " DML, the column must be a integer data type containing yyyymmdd values")
        args_chunk_r_grp.add_argument(
            "--chunk_rows", dest="chunk_rows", required=True
            , type=yb_common.intRange(1,9223372036854775807)
            , help="the minimum rows that each chunk should contain")

        args_chunk_o_grp = self.common.args_parser.add_argument_group(
            'chunking optional arguments')
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
    cdml = chunk_dml_by_yyyymmdd_integer()

    sys.stdout.write('-- Running DML chunking.\n')
    cdml.exec()
    cdml.cmd_results.write(tail='-- Completed DML chunking.\n')

    exit(cdml.cmd_results.exit_code)


if __name__ == "__main__":
    main()