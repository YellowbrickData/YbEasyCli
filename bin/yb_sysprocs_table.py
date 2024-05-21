#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_table.py [options]

PURPOSE:
      Report all user tables in all databases.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_table.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_table(SPReportUtil):
    config = {
        'description': 'Report all user tables in all databases.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|table_name'
        , 'db_filter_args': {'db_name':'db_name', 'schema_name':'schema_name', 'table_name':'table_name', 'owner_name':'owner_name'}
        , 'optional_args_multi': ['db_name', 'schema_name', 'table_name', 'owner_name']
        , 'usage_example_extra': {'cmd_line_args': "--db_name_like '%dze%'" } }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_table().execute())
    exit(0)

if __name__ == "__main__":
    main()