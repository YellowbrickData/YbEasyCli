#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_backup_chains.py [options]

PURPOSE:
      Report existing backup chains with creating and snapshot info.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_backup_chains.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_backup_chains(SPReportUtil):
    config = {
        'description': 'Existing backup chains with creating and snapshot info report.'
        , 'report_sp_location': 'sysviews'
        , 'optional_args_multi': ['database', 'chain_name']
        , 'optional_args_single': []
        , 'db_filter_args': {'database':'database_name', 'chain_name':'chain_name'} }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('arguments')
        args_optional_filter_grp.add_argument("--trunc_policy"
            , action="store_true"
            , help = "Truncate the chain policy desc at 6 chars.")

    def execute(self):
        return self.build({
            '_trunc_policy' : self.args_handler.args.trunc_policy
            , '_yb_util_filter' : self.db_filter_sql() })

def main():
    print(report_backup_chains().execute())
    exit(0)

if __name__ == "__main__":
    main()
