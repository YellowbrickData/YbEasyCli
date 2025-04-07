#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_column.py [options]

PURPOSE:
      Cross-database column metadata for tables and views similar to \\d

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_column_dstr.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_column(SPReportUtil):
    """Cross-database column metadata for tables and views similar to \\d."""
    config = {
        'description': 'Cross-database column metadata for tables and views similar to \\d.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|rel_name|col_name'
        , 'optional_args_multi': ['database', 'schema', 'table', 'column']
        , 'db_filter_args': {'database':'db_name', 'schema':'schema_name', 'table':'rel_name', 'column':'col_name'}
        , 'usage_example_extra': {'cmd_line_args': "--schema dev --table store --column invoice_total" } }

    def execute(self):
        return self.build({'_yb_util_filter'  : '%s' % self.db_filter_sql() })

def main():
    print(report_column().execute())
    exit(0)

if __name__ == "__main__":
    main()