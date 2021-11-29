#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_sysviews.py [options]

PURPOSE:
      Names and arguments for all installed sysviews procedures.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_sysviews.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_sysviews(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Names and arguments for all installed sysviews procedures.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'schema|procedure|arguments' }

    def execute(self):
        if self.db_conn.ybdb['is_super_user']:
            self.db_conn.env['conn_db'] = 'sysviews'
        return self.build()

def main():
    print(report_sysviews().execute())
    exit(0)

if __name__ == "__main__":
    main()