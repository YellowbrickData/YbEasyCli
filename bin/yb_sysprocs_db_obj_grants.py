#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_db_obj_grants.py [options]

PURPOSE:
      Provides details on database object grants.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_db_obj_grants.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class db_obj_grants(SPReportUtil):
    """Issue the ybsql commands used to retrieve database object grants report."""
    config = {
        'description': 'Provides details on database object grants.'
        , 'report_sp_location': 'sysviews'
    }

    def additional_args(self):
        args_db_obj_grants_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_db_obj_grants_grp.add_argument(
            "--user_ilike", required=True
            , help="filter for specific user pattern")
        args_db_obj_grants_grp.add_argument(
            "--schema_ilike", default='%', help="optional filter for schema pattern")
        args_db_obj_grants_grp.add_argument(
            "--obj_name_ilike", default='%', help="optional filter for object name pattern")
        args_db_obj_grants_grp.add_argument(
            "--show_sql", type=int, default=0, help="optional flag to display SQL query")


    def execute(self):
        return self.build({
            '_user_ilike': self.args_handler.args.user_ilike
            , '_schema_ilike': self.args_handler.args.schema_ilike
            , '_obj_name_ilike': self.args_handler.args.obj_name_ilike
            , '_show_sql': self.args_handler.args.show_sql})

def main():
    print(db_obj_grants().execute())
    exit(0)

if __name__ == "__main__":
    main()
