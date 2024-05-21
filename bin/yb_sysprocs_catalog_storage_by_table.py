#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_catalog_storage_by_table.py [options]

PURPOSE:
      Report size of catalog tables across all databases.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_catalog_storage_by_table.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_catalog_storage_by_table(SPReportUtil):
    config = {
        'description': 'Size of catalog tables across all databases.'
        , 'report_sp_location': 'sysviews'
        , 'optional_args_multi': ['database', 'schema', 'table']
        , 'optional_args_single': []
        , 'db_filter_args': {'database':'db_name', 'schema':'table_schema', 'table':'table_name'} }

    def execute(self):
        return self.build({
            '_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_catalog_storage_by_table().execute())
    exit(0)

if __name__ == "__main__":
    main()