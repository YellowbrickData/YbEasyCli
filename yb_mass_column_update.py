#!/usr/bin/env python3
"""
USAGE:
      yb_mass_column_update.py [database] [options]

PURPOSE:
      Update the value of multiple columns.

OPTIONS:
      See the command line help message for all options.
      (yb_mass_column_update.py --help)

Output:
      The update statements for the requested set of columns.
"""
import sys

import yb_common
from yb_util import util

class mass_column_update(util):
    """Issue the ybsql command used to list the column names comprising an
    object.
    """
    config = {
        'description': (
            'Update the value of multiple columns.'
            '\n'
            '\nnote:'
            '\n  Mass column updates may cause performance issues due to the change '
            '\n  of how the data is ordered in storage.')
        , 'optional_args_single': []
        , 'optional_args_multi': ['owner', 'schema', 'table', 'column', 'datatype']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' --update_where_clause \"<columnname> = 'NULL'\" --set_clause NULL --"
            , 'file_args': [util.conn_args_file] }
        , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname', 'datatype':'datatype'} }

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_mass_column_update_p'
            , args = {
                'a_update_where_clause' : self.args_handler.args.update_where_clause
                , 'a_set_clause' : self.args_handler.args.set_clause
                , 'a_column_filter_clause' : self.db_filter_sql()
                , 'a_exec_updates' : ('TRUE' if self.args_handler.args.exec_updates else 'FALSE')}
            , pre_sql = self.args_handler.args.pre_sql
            , post_sql = self.args_handler.args.post_sql)

    def additional_args(self):
        args_mass_r_grp = self.args_handler.args_parser.add_argument_group('required mass update arguments')
        args_mass_r_grp.add_argument(
            "--update_where_clause", required=True
            , help=("update column only if this boolean clause is satisfied, like: "
                "'LENGTH(<column>)<>LENGTH(RTRIM(<column>))',  "
                "Note: the special use of the string '<column>' ")
        )
        args_mass_r_grp.add_argument(
            "--set_clause", required=True
            , help=("Set the column to this value, Like; "
                "'RTRIM(<column>)', "
                "Note: the special use of the string '<column>' ")
        )

        args_mass_o_grp = self.args_handler.args_parser.add_argument_group('optional mass update arguments')
        args_mass_o_grp.add_argument(
            "--exec_updates"
            , action='store_true'
            , help=("defaults to False and only prints the update statements. When set "
                "to True, execute the update statements.")
        )
        args_mass_o_grp.add_argument("--pre_sql", default=''
            , help="SQL to run before the chunking DML, only runs if execute_chunk_dml is set")
        args_mass_o_grp.add_argument("--post_sql", default=''
            , help="SQL to run after the chunking DML, only runs if execute_chunk_dml is set")

    def additional_args_process(self):
        if '<column>' not in self.args_handler.args.update_where_clause:
            yb_common.common.error("UPDATE_WHERE_CLAUSE must contain the string '<column>'")

        if not self.args_handler.args.exec_updates:
            self.args_handler.args.pre_sql = ''
            self.args_handler.args.post_sql = ''

        self.args_handler.db_filter_args.schema_set_all_if_none()

def main():
    mcu = mass_column_update()

    sys.stdout.write('-- Running mass column update.\n')
    mcu.execute()
    mcu.cmd_results.write(tail='-- Completed mass column update.\n')

    exit(mcu.cmd_results.exit_code)


if __name__ == "__main__":
    main()