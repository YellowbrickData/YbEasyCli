#!/usr/bin/env python3
"""
USAGE:
      yb_analyze_columns.py [database] table [options]

PURPOSE:
      Analyze the data content of a table's column/s.

OPTIONS:
      See the command line help message for all options.
      (yb_analyze_columns.py --help)

Output:
      Various column statistics for desired table/s column/s.
"""
import sys

from yb_util import util

class analyze_columns(util):
    """Issue the ybsql command used to analyze the data content of a table's column/s
    """

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sys.stdout.write('-- Running column analysis.\n')

        cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_analyze_columns_p'
                , args = {
                    'a_dbname' : self.db_conn.database
                    , 'a_tablename' : self.args_handler.args.table
                    , 'a_filter_clause' : filter_clause})

        cmd_results.write(tail='-- Completed column analysis.\n')

        exit(cmd_results.exit_code)


def main():
    acs = analyze_columns()
    acs.execute()

if __name__ == "__main__":
    main()