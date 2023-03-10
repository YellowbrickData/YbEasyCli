#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_rowstore_by_table.py [options]

PURPOSE:
      Size of rowstore data in user tables across all databases.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_rowstore_by_table.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_rowstore_by_table(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Size of rowstore data in user tables across all databases.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|table_name' }

    def execute(self):
        return self.build()

def main():
    print(report_rowstore_by_table().execute())
    exit(0)

if __name__ == "__main__":
    main()