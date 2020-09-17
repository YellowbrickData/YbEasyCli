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

class chunk_dml_by_integer:
    """Issue the ybsql command used to create/execute DML chunked by an integer column
    """

    def __init__(self):

        common = self.init_common()

        sys.stdout.write('-- Running DML chunking.\n')

        cmd_results = common.call_stored_proc_as_anonymous_block(
            'yb_chunk_dml_by_integer_p'
            , args = {
                'a_table_name' : common.args.table
                , 'a_integer_column_name' : common.args.column
                , 'a_dml' : common.args.dml
                , 'a_min_chunk_size' : common.args.chunk_rows
                , 'a_verbose' : ('TRUE' if common.args.verbose_chunk_off else 'FALSE')
                , 'a_add_null_chunk' : ('TRUE' if common.args.null_chunk_off else 'FALSE')
                , 'a_print_chunk_dml' : ('TRUE' if common.args.print_chunk_dml else 'FALSE')
                , 'a_execute_chunk_dml' : ('TRUE' if common.args.execute_chunk_dml else 'FALSE')}
            , pre_sql = common.args.pre_sql
            , post_sql = common.args.post_sql)

        cmd_results.write(tail='-- Completed DML chunking.\n')

        exit(cmd_results.exit_code)

    def add_args(self, common):
        common.args_process_init(
            description=('Chunk DML by INTEGER column.')
            , positional_args_usage='')

        common.args_add_optional()
        common.args_add_connection_group()

        args_chunk_r_grp = common.args_parser.add_argument_group('chunking required arguments')
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
                " DML, the column must be a integer data type")
        args_chunk_r_grp.add_argument(
            "--chunk_rows", dest="chunk_rows", required=True
            , type=yb_common.intRange(1,9223372036854775807)
            , help="the minimum rows that each chunk should contain")

        args_chunk_o_grp = common.args_parser.add_argument_group('chunking optional arguments')
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

        if '<chunk_where_clause>' not in common.args.dml:
            sys.stderr.write("DML must contain the string '<chunk_where_clause>'\n")
            exit(1)

        if not common.args.execute_chunk_dml:
            common.args.pre_sql = ''
            common.args.post_sql = ''

        return common


chunk_dml_by_integer()