#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_schema.py [options]

PURPOSE:
      All user schemas across all databases.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_schema.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_schema(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'All user schemas across all databases.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'db_name|schema_name'
        , 'db_filter_args': {'database':'db_name', 'schema':'schema_name'}
        , 'optional_args_multi': ['database', 'schema']
        , 'usage_example_extra': {'cmd_line_args': "--database_NOTlike 'z%'" } }

    def execute(self):
        return self.build({'_yb_util_filter'  : '%s' %self.db_filter_sql() })

def main():
    # YB 4 looks to be throwing an eronious warning with these sys views which I'm stripping
    strip_warnings = [r'WARNING:  Error querying database metadata.*(RETURN QUERY|FOR over EXECUTE statement)']
    print(report_schema(strip_warnings=strip_warnings).execute())
    exit(0)

if __name__ == "__main__":
    main()