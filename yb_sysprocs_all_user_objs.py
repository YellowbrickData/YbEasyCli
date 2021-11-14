#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_all_user_objs.py [options]

PURPOSE:
      Report all user objects in all databases with owner and ACL detail.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_all_user_objs.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_all_user_objs(SPReportUtil):
    """Issue the ybsql commands used to create the user objects report."""
    config = {
        'description': 'Report all user objects in all databases with owner and ACL details.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|obj_type|schema_name|obj_name'
        , 'optional_args_multi': ['database', 'schema', 'object', 'owner']
        , 'db_filter_args': {'database':'db_name', 'schema':'schema_name', 'object':'obj_name', 'owner':'owner_name'}
        , 'usage_example_extra': {'cmd_line_args': "--database_like '%dze%' --schema_in dev --object_type TABLE SCHEMA" } }

    def additional_args(self):
        #TODO can we get DATABASE ACls
        args_report_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_report_grp.add_argument(
            "--object_type", nargs="+"
            , choices=['SCHEMA', 'TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'UDF', 'DFLTACL']
            , help="filter selected DB object types")

    def execute(self):
        filter_clause = self.db_filter_sql()
        if self.args_handler.args.object_type:
            filter_clause += " AND obj_type IN ('%s')" % "','".join(self.args_handler.args.object_type)
        return self.build({'_yb_util_filter' : filter_clause })

def main():
    print(report_all_user_objs().execute())
    exit(0)

if __name__ == "__main__":
    main()