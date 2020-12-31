#!/usr/bin/env python3
"""
USAGE:
      yb_find_columns.py [database] [options]

PURPOSE:
      List all columns found for the provided filter.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The column names and column attributes for filtered columns.
"""
from yb_util import util

class find_columns(util):
    """Issue the ybsql command used to list the column names comprising an
    object.
    """
    config = {
        'description': 'List column names and column attributes for filtered columns.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'table', 'column', 'datatype']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' 'TIME%' --"
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['table_path', 'schema_path', 'column', 'ordinal', 'data_type', 'table', 'schema', 'database']
        , 'output_tmplt_default': '-- Table: <table_path>, Column: <column>, Table Ordinal: <ordinal>, Data Type: <data_type>'
        , 'db_filter_args':
            {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname', 'datatype':'datatype'} }

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_find_columns_p'
                , args = {
                    'a_column_filter_clause' : filter_clause } )

        self.apply_template()

def main():
    fcs = find_columns()
    fcs.execute()

    fcs.cmd_results.write()

    exit(fcs.cmd_results.exit_code)


if __name__ == "__main__":
    main()