#!/usr/bin/env python3
"""
USAGE:
      yb_report_wlm_active_rule.py [options]

PURPOSE:
      Current active WLM profile rules.

OPTIONS:
      See the command line help message for all options.
      (yb_report_wlm_active_rule.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil
from yb_common import ArgIntRange

class report_wlm_active_rule(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Current active WLM profile rules.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'rule_order|rule_type|profile_name' }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_log_query_grp.add_argument(
            "--expr_chars", metavar='CHARS', type=ArgIntRange(1,60000), default=32
            , help="number of expression characters to display, defaults to: 32")

    def execute(self):
        return self.build({'_expr_chars': self.args_handler.args.expr_chars})

def main():
    print(report_wlm_active_rule().execute())
    exit(0)

if __name__ == "__main__":
    main()