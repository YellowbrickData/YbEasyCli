#!/usr/bin/env python3
"""
USAGE:
      report_storage_by_table.py [options]

PURPOSE:
      Table storage report.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_storage_by_table.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_storage_by_table(SPReportUtil):
    """Issue the ybsql commands used to generate a table storage report."""
    config = {
        'description': 'Table storage report.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|table_name'
        , 'optional_args_multi': ['database', 'schema', 'table']
        , 'db_filter_args': {'database':'d.name', 'schema':'s.name', 'table':'ts.table_name'}
        , 'usage_example_extra': {'cmd_line_args': '--database_in dze_db1 --report_order_by uncmpr_gb DESC table_name' } }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_storage_by_table().execute())
    exit(0)

if __name__ == "__main__":
    main()