#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_load.py [options]

PURPOSE:
      Transformed subset of sys.load columns for active bulk loads.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_load.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_load(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Transformed subset of sys.load columns for active bulk loads.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'start_time' }

    def execute(self):
        return self.build()

def main():
    print(report_load().execute())
    exit(0)

if __name__ == "__main__":
    main()