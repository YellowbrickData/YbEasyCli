#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_lock.py [options]

PURPOSE:
      Report all blocked and blocking sessions.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_lock.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_lock(SPReportUtil):
    """Issue the ybsql commands used to generate a blocking sessions report."""
    config = {
        'description': 'Blocking sessions report.'
        , 'report_sp_location': 'sysviews'
        }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('arguments')
        args_optional_filter_grp.add_argument("--all"
            , action="store_true"
            , help = "show all locks, including non-blocking ones")

    def execute(self):
        return self.build({'_all' : self.args_handler.args.all })

def main():
    print(report_lock().execute())
    exit(0)

if __name__ == "__main__":
    main()
