#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_catalog_storage_by_db.py [options]

PURPOSE:
      Report size of catalog tables across all databases by database.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_catalog_storage_by_db.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_catalog_storage_by_db(SPReportUtil):
    config = {
        'description': 'Size of catalog tables across all databases by database report.'
        , 'report_sp_location': 'sysviews'
        , 'optional_args_multi': ['database']
        , 'optional_args_single': []
        , 'db_filter_args': {'database':'name'} }

    def execute(self):
        return self.build({
            '_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_catalog_storage_by_db().execute())
    exit(0)

if __name__ == "__main__":
    main()