#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_view_validate.py [options]

PURPOSE:
      Iterate over views to determine which ones have missing dependencies due to late bound views.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_view_validate.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_view_validate(SPReportUtil):
    config = {
        'description': 'Iterate over views to determine which ones have missing dependencies due to late bound views.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name|view_name'
        , 'db_filter_args': {'owner_name':'owner_name', 'db_name':'db_name', 'schema_name':'schema_name', 'view_name':'view_name'}
        , 'optional_args_multi': ['owner_name', 'db_name', 'schema_name', 'view_name'] }

    def execute(self):
        return self.build({'_yb_util_filter' : self.db_filter_sql() })

def main():
    # YB 4 looks to be throwing an eronious warning with these sys views which I'm stripping
    strip_warnings = [r'WARNING:  Error querying database metadata.*(RETURN QUERY|FOR over EXECUTE statement)']
    print(report_view_validate(strip_warnings=strip_warnings).execute())
    exit(0)

if __name__ == "__main__":
    main()