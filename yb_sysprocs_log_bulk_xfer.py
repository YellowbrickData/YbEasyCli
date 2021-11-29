#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_bulk_xfer.py [options]

PURPOSE:
      Transformed subset active bulk transfers (ybload & ybunload) from sys.load and sys.unload.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_bulk_xfer.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_log_bulk_xfer(SPReportUtil):
    """Issue the ybsql commands used to create the user objects report."""
    config = {
        'description': 'Transformed subset active bulk transfers (ybload & ybunload) from sys.load and sys.unload.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'start_time' }

    def execute(self):
        return self.build()

def main():
    print(report_log_bulk_xfer().execute())
    exit(0)

if __name__ == "__main__":
    main()