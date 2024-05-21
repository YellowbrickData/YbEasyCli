#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_rowstore.py [options]

PURPOSE:
      Rowstore overal metrics including size of data in user tables.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_rowstore.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_rowstore(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Rowstore overal metrics including size of data in user tables.'
        , 'report_sp_location': 'sysviews' }

    def execute(self):
        return self.build()

def main():
    print(report_rowstore().execute())
    exit(0)

if __name__ == "__main__":
    main()