#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_session.py [options]

PURPOSE:
      Current session state details.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_session.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_session(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Current session state details.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'query_id'
        , 'usage_example_extra': {'cmd_line_args': "--report_order_by pid" } }

    def execute(self):
        return self.build()

def main():
    print(report_session().execute())
    exit(0)

if __name__ == "__main__":
    main()