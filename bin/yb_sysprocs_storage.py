#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_storage.py [options]

PURPOSE:
      Aggregated summary of appliance storage report.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_storage.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_storage(SPReportUtil):
    """Issue the ybsql commands used to generate a summary of appliance storage report."""
    config = {
        'description': 'Aggregated summary of appliance storage report.'
        , 'report_sp_location': 'sysviews' }

    def execute(self):
        return self.build({})

def main():
    print(report_storage().execute())
    exit(0)

if __name__ == "__main__":
    main()