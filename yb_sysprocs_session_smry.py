#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_session_smry.py [options]

PURPOSE:
      Current sessions aggregated by db, user, state, app, ip, etc...

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_session_smry.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_session_smry(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Current sessions aggregated by db, user, state, app, ip, etc...'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'state|db_name|user_name|app_name' }

    def execute(self):
        return self.build()

def main():
    print(report_session_smry().execute())
    exit(0)

if __name__ == "__main__":
    main()