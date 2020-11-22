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

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_find_columns_p'
                , args = {
                    'a_column_filter_clause' : filter_clause
                }
            )

        self.apply_template()

def main():
    fcs = find_columns()
    fcs.execute()

    fcs.cmd_results.write()

    exit(fcs.cmd_results.exit_code)


if __name__ == "__main__":
    main()