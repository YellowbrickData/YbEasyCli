#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_steps.py [options]

PURPOSE:
      Completed statements actual vs plan metrics by plan node.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_steps.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_log_query_steps(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Completed statements actual vs plan metrics by plan node.'
        , 'report_sp_location': 'sysviews'
        , 'usage_example_extra': {'cmd_line_args': "--query_id 20190401" } }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_grp.add_argument("--query_id", type=int, required=True, help="query_id to report on")

    def execute(self):
        return self.build({'_query_id_in': self.args_handler.args.query_id})

def main():
    print(report_log_query_steps().execute())
    exit(0)

if __name__ == "__main__":
    main()