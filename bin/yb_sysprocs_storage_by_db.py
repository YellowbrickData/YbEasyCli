#!/usr/bin/env python3
"""
USAGE:
      report_storage_by_db.py [options]

PURPOSE:
      Storage of committed blocks in user tables aggregated by database.

OPTIONS:
      See the command line help message for all options.
      (report_storage_by_db.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_storage_by_db(SPReportUtil):
    """Issue the ybsql commands used to generate a database storage report."""
    config = {
        'description': 'Table storage report.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'cmpr_gb|DESC'
        , 'db_filter_args': {'database':'db_name'}
        , 'optional_args_multi': ['database'] }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_storage_by_db().execute())
    exit(0)

if __name__ == "__main__":
    main()