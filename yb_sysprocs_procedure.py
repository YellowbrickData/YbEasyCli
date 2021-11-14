#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_procedure.py [options]

PURPOSE:
      User created stored procedures.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_procedure.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_procedure(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'User created stored procedures.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|proc_name'
        , 'optional_args_multi': ['database', 'schema', 'procedure']
        , 'db_filter_args': {'database':'db_name', 'schema':'schema_name', 'procedure':'proc_name'}
        , 'usage_example_extra': {'cmd_line_args': "--database_like '%dze%'" } }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_procedure().execute())
    exit(0)

if __name__ == "__main__":
    main()