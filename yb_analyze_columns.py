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
from tabulate import tabulate

from yb_util import util

class analyze_columns(util):
    """Issue the ybsql command used to analyze the data content of a table's column/s
    """

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_analyze_columns_p'
            , args = {
                'a_dbname' : self.db_conn.database
                , 'a_tablename' : self.args_handler.args.table
                , 'a_filter_clause' : filter_clause
                , 'a_level' : self.args_handler.args.level
                , 'a_delimited_output' : self.a_delimited_output})

    def additional_args(self):
        args_chunk_o_grp = self.args_handler.args_parser.add_argument_group(
            'optional analyze argument')
        args_chunk_o_grp.add_argument("--level", type=int, choices=range(1, 4)
            , help="level of analysis, 1 - estimate, 2 - count, 3 - groups, group"
                " analysis may take a siginificant amount of time to complete"
                " on large tables, group also forces expanded output format"
                ", defaults to estimate", default=1)
        args_chunk_o_grp.add_argument("--output_format", type=int, choices=range(1, 4)
            , help="1 - formatted table, 2 - delimited, 3 - expanded, defaults to table", default=1)

    def additional_args_process(self):
        if self.args_handler.args.level == 3:
            self.args_handler.args.output_format = 3
        self.a_delimited_output = (self.args_handler.args.output_format != 3)

def main():
    acs = analyze_columns()

    sys.stdout.write('-- Running column analysis.\n')

    acs.execute()

    if acs.cmd_results.stdout != '':
        if acs.args_handler.args.output_format == 1:
            rows = []
            headers = True
            for line in acs.cmd_results.stdout.split('\n'):
                if line == '':
                    continue
                row = line.split('|')
                if headers:
                    row = [col.replace('_', '\n') for col in row]
                    headers = False
                rows.append(row)
            print(tabulate(rows, headers="firstrow"))
        else:
            sys.stdout.write(acs.cmd_results.stdout)
    if acs.cmd_results.stderr != '':
        common.error(acs.cmd_results.stderr, no_exit=True)
    else:
        sys.stdout.write('-- Completed column analysis.\n')

    exit(acs.cmd_results.exit_code)

if __name__ == "__main__":
    main()