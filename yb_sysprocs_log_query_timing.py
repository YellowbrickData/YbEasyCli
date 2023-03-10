#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_timing.py [options]

PURPOSE:
      Details on completed backend statements.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_timing.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_log_query_timing(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Details on completed backend statements.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'query_id' }

    def execute(self):
        return self.build()

def main():
    print(report_log_query_timing().execute())
    exit(0)

if __name__ == "__main__":
    main()