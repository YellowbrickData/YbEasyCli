#!/usr/bin/env python3
"""
USAGE:
      yb_find_columns.py [options]

PURPOSE:
      List all columns found for the provided filter.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The column names and column attributes for filtered columns.
"""
from yb_common import StoredProc, Util

class find_columns(Util):
    """Issue the ybsql command used to list the column names comprising an object.
    """
    config = {
        'description': 'List column names and column attributes for filtered columns.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'table', 'column', 'datatype']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' 'TIME%' --"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['column_path', 'table_path', 'schema_path', 'column', 'ordinal', 'data_type', 'table_ordinal', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '-- Table: {table_path}, Column: {column}, Table Ordinal: {table_ordinal}, Data Type: {data_type}'
        , 'db_filter_args':
            {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname', 'datatype':'datatype'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        self.cmd_results = StoredProc('yb_find_columns_p', self.db_conn).call_proc_as_anonymous_block(
                args = {
                    'a_column_filter_clause' : self.db_filter_sql() } )

        rows_as_dict_str = ''
        self.col_ct = 0
        if len(self.cmd_results.stdout.strip()):
            for line in self.cmd_results.stdout.strip().split('\n'):
                self.col_ct += 1
                row = line.split('|')
                rows_as_dict_str += ( ('' if self.col_ct == 1 else ', ') + '{'
                    + ("'ordinal': %d" % self.col_ct)
                    + (', "table_ordinal": ""\" %s ""\" ' % row[0])
                    + (', "database": ""\" %s ""\" '      % row[1])
                    + (', "schema": ""\" %s ""\" '        % row[2])
                    + (', "table": ""\" %s ""\" '         % row[3])
                    + (', "column": ""\" %s ""\" '        % row[4])
                    + (', "data_type": ""\" %s ""\" '     % row[5])
                    + (', "owner": ""\" %s ""\" '         % row[6]) + '}\n' )

        return self.apply_template(rows_as_dict_str, exec_output=self.args_handler.args.exec_output)

def main():
    fcs = find_columns()
    print('-- Running: yb_find_columns')

    print(fcs.execute().strip())

    print('-- %d column/s found' % fcs.col_ct)
    exit(fcs.cmd_results.exit_code)


if __name__ == "__main__":
    main()