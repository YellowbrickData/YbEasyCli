#!/usr/bin/env python3
"""
USAGE:
      yb_sysproc_wlm_state.py [options]

PURPOSE:
      Returns current active WLM profile state metrics by pool.

OPTIONS:
      See the command line help message for all options.
      (yb_sysproc_wlm_state.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_wlm_state(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Returns current active WLM profile state metrics by pool.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'pool_id|req_mb|slots' }

    def execute(self):
        return self.build()

def main():
    print(report_wlm_state().execute())
    exit(0)

if __name__ == "__main__":
    main()