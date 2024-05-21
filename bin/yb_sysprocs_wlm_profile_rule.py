#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_wlm_profile_rule.py [options]

PURPOSE:
      Current active or named WLM detailed profile rules.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_wlm_profile_rule.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil
from yb_common import ArgIntRange

class report_wlm_profile_rule(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Current active or named WLM detailed profile rules.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'rule' }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_log_query_grp.add_argument(
            "--profile_name", default=''
            , help="profile to report on, defaults to the active profile")

    def execute(self):
        return self.build({'_profile_name': self.args_handler.args.profile_name})

def main():
    print(report_wlm_profile_rule().execute())
    exit(0)

if __name__ == "__main__":
    main()