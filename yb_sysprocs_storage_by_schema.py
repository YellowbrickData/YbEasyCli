#!/usr/bin/env python3
"""
USAGE:
      report_storage_by_schema.py [options]

PURPOSE:
      Storage summary by schema across one or more databases.

OPTIONS:
      See the command line help message for all options.
      (report_storage_by_schema.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_storage_by_schema(SPReportUtil):
    """Issue the ybsql commands used to generate a summary by schema storage report."""
    config = {
        'description': 'Storage summary by schema across one or more databases.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name'
        , 'optional_args_multi': ['database', 'schema']
        , 'db_filter_args': {'database':'d.name', 'schema':'s.name'}
        , 'usage_example_extra': {'cmd_line_args': '--database_in dze_db1 --report_order_by uncmpr_gb DESC table_name'} }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_storage_by_schema().execute())
    exit(0)

if __name__ == "__main__":
    main()