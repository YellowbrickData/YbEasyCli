#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_pivot.py [options]

PURPOSE:
      Queries for the last week aggregated by hour for use in WLM pivot table analysis.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_pivot.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil
from yb_common import ArgDate

class report_log_query_pivot(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Queries for the last week aggregated by hour for use in WLM pivot table analysis.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'yyyy|m|mon|week_begin|date|dow|day|hour|pool|status|user_name|app_name|tags|stmt_type|gb_grp|confidence|est_gb_grp|spill' }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_grp.add_argument("--from_date", type=ArgDate(), help=("starting DATE(YYYY-MM-DD) "
            "  of statments to analyze, defaults to begining of previous week (Sunday).") )

    def execute(self):
        args = {}
        if self.args_handler.args.from_date:
              args['_from_ts'] = self.args_handler.args.from_date
        return self.build(args)

def main():
    print(report_log_query_pivot().execute())
    exit(0)

if __name__ == "__main__":
    main()