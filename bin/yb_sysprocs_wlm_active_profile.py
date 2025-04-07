#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_wlm_active_profile.py [options]

PURPOSE:
      Returns current active WLM profile configuration details by pool.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_wlm_active_profile.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_wlm_active_profile(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Returns current active WLM profile configuration details by pool.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'profile|name' }

    def execute(self):
        return self.build()

def main():
    print(report_wlm_active_profile().execute())
    exit(0)

if __name__ == "__main__":
    main()