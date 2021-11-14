#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_smry.py [options]

PURPOSE:
      Aggregated subset of the sys.log_query data.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_smry.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil
from yb_common import ArgDate

class report_log_query_smry(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Aggregated subset of the sys.log_query data.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'week_begin|pool' }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_grp.add_argument("--submit_date", type=ArgDate(), help=("the DATE(YYYY-MM-DD) for the minimum"
            "  submit_time to use for the report, defaults to midnight of the first day of the current week.") )

    def execute(self):
        args = {}
        if self.args_handler.args.submit_date:
              args['_submit_ts'] = self.args_handler.args.submit_date
        return self.build(args)

def main():
    print(report_log_query_smry().execute())
    exit(0)

if __name__ == "__main__":
    main()