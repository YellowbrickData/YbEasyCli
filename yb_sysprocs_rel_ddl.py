#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_rel_ddl.py [options]

PURPOSE:
      Reports ddl for user table(s) as sequetial varchar rows.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_rel_ddl.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_rel_ddl(SPReportUtil):
    config = {
        'description': 'Reports ddl for user table(s) as sequetial varchar rows.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'line'
        , 'db_filter_args': {'owner':'o.owner_name', 'database':'n.database_name', 'schema':'n.nspname', 'table':'r.table_name'}
        , 'optional_args_multi': ['owner', 'database', 'schema', 'table'] }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    # YB 4 looks to be throwing an eronious warning with these sys views which I'm stripping
    strip_warnings = [r'WARNING:  Error querying database metadata.*(RETURN QUERY|FOR over EXECUTE statement)']
    print(report_rel_ddl(strip_warnings=strip_warnings).execute())
    exit(0)

if __name__ == "__main__":
    main()