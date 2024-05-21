#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query.py [options]

PURPOSE:
      Details on completed backend statements.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_common import ArgIntRange
from yb_sp_report_util import SPReportUtil

class report_log_query(SPReportUtil):
    """Issue the ybsql commands used to completed backend statements report."""
    config = {
        'description': 'Details on completed backend statements.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'query_id'
        , 'usage_example_extra': {'cmd_line_args': "--query_chars 20 --report_include query_id submit_time query_text --report_order_by submit_time DESC" } }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_log_query_grp.add_argument(
            "--predicate", default=''
            , help=("optional sql predicate used to filter the queries returned in the report" ) )
        args_log_query_grp.add_argument(
            "--query_chars", metavar='CHARS'
            , type=ArgIntRange(1,60000), default=32
            , help="number of query_text characters to display, defaults to: 32")

    def execute(self):
        return self.build({
            '_pred': self.args_handler.args.predicate
            , '_query_chars': self.args_handler.args.query_chars})

def main():
    print(report_log_query().execute())
    exit(0)

if __name__ == "__main__":
    main()

