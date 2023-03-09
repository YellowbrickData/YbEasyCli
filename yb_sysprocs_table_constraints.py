#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_table_constraints.py [options]

PURPOSE:
      Existing constraints on user tables as per information_schema.table_constraints.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_table_constraints.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_table_constraints(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Existing constraints on user tables as per information_schema.table_constraints.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'constraint_catalog'
        , 'db_filter_args': {'database':'db_name'}
        , 'optional_args_multi': ['database']
        , 'usage_example_extra': {'cmd_line_args': "--database_like '%dze%'" } }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_table_constraints().execute())
    exit(0)

if __name__ == "__main__":
    main()