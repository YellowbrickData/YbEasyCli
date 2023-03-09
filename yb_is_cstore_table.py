#!/usr/bin/env python3
"""
USAGE:
      yb_is_cstore_table.py [options]

PURPOSE:
      Determine if a table is stored as a column store table

OPTIONS:
      See the command line help message for all options.
      (yb_is_cstore_table.py --help)

Output:
      True/False
"""
from yb_common import Common, StoredProc, Util

class is_cstore_table(Util):
    """Issue the ybsql command used to determine if a table is stored as a column store table.
    """
    config = {
        'description': 'Determine if a table is stored as a column store table.'
        , 'required_args_single': ['table']
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --table sys.blade --'
            , 'file_args': [Util.conn_args_file] } }

    def execute(self):
        self.cmd_results = StoredProc('yb_is_cstore_table_p', self.db_conn).call_proc_as_anonymous_block(
            args = {
                'a_table' : Common.quote_object_paths(self.args_handler.args.table)})

        self.cmd_results.write()
        print(self.cmd_results.proc_return)

def main():
    iscst = is_cstore_table()

    iscst.execute()

    exit(iscst.cmd_results.exit_code)


if __name__ == "__main__":
    main()