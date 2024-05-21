#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_replica_bulk_action.py [options]

PURPOSE:
      Build SQL script to PAUSE, RESUME, or RESTART replicas.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The report is an SQL script.
"""
from yb_sp_report_util import SPReportUtil

class replica_bulk_action(SPReportUtil):
    """Issue the ybsql commands used to generate a table skew report."""
    config = {
        'description': 'Build SQL script to PAUSE, RESUME, or RESTART replicas.'
        , 'report_sp_location': 'sysviews'
       , 'report_default_order': '--SQL'
        , 'optional_args_single': []
        , 'optional_args_multi': ['replica']
        , 'db_filter_args': {'replica':'r.name'}
        , 'usage_example_extra': {
            'cmd_line_args': '@$HOME/skew_report.args --action RESTART'
             } }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('arguments')
        args_optional_filter_grp.add_argument("--action"
            , choices = ('PAUSE','RESUME','RESTART'), default='RESTART'
            , help = "data size unit, defaults to RESTART")
        args_optional_filter_grp.add_argument("--stagger_minutes"
            , type = float, default = 2.0
            , help = "The number of minutes to wait between RESUME or RESTART of replicas.")

#        args_optional_filter_grp.add_argument("--skew_pct_min"
#            , type = float
#            , help = "limit the report by the selected column with the specified minimum percent")

    def execute(self):
        return self.build({
              '_yb_util_filter'  : self.db_filter_sql()
            , '_action'          : self.args_handler.args.action
            , '_stagger_minutes' : self.args_handler.args.stagger_minutes})

def main():
    print(replica_bulk_action().execute())
    exit(0)

if __name__ == "__main__":
    main()
