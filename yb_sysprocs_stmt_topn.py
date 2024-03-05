#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_stmt_topn.py [options]

PURPOSE:
      The top <n> (i.e. worst) performing statements across multiple columns.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_stmt_topn.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_stmt_topn(SPReportUtil):
    config = {
        'description': 'The top <n> (i.e. worst) performing statements across multiple columns.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'exe_sec|run_sec|spl_wrt_mb'
        , 'db_filter_args': {'db_name':'db_name', 'schema_name':'schema_name', 'table_name':'table_name', 'owner_name':'owner_name'}
        , 'optional_args_multi': ['db_name', 'schema_name', 'table_name', 'owner_name']
        , 'usage_example_extra': {'cmd_line_args': "--db_name_like '%dze%'" } }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_stmt_topn().execute())
    exit(0)

if __name__ == "__main__":
    main()