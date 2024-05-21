#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_column_stats.py [options]

PURPOSE:
      Table column metdata including estimates from statistics.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_column_stats.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_column_stats(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Table column metdata including estimates from statistics.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'table_schema|table_name'
        , 'required_args_single': ['database']
        , 'optional_args_multi': ['schema', 'table']
        , 'db_filter_args': {'database':'db_name', 'schema':'table_schema', 'table':'table_name'}
        , 'usage_example_extra': {'cmd_line_args': "--database acme --schema_in dev --table_like 'cust%'" } }

    def execute(self):
        return self.build({
            '_db_name': self.args_handler.args.database
            , '_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_column_stats().execute())
    exit(0)

if __name__ == "__main__":
    main()