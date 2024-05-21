#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_wlm_profile_rule.py [options]

PURPOSE:
      Returns SQL to create a WLM profile.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_wlm_profile_rule.py --help)

Output:
      SQL to build a WLM profile.
"""
from yb_sp_report_util import SPReportUtil

class report_wlm_profile_sql(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Returns SQL to create a WLM profile.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'code' }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('arguments')
        args_log_query_grp.add_argument(
            "--profile_name", required=True
            , help="WLM profile to generate SQL for")

    def execute(self):
        return self.build({'_profile_name': self.args_handler.args.profile_name})

def main():
    print(''.join(report_wlm_profile_sql().execute().splitlines(keepends=True)[3:]))
    exit(0)

if __name__ == "__main__":
    main()