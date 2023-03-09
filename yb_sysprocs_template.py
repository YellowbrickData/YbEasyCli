#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_{name}.py [options]

PURPOSE:
      {description}

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_{name}.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_{name}(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {{
        'description': '{description}'
        , 'report_sp_location': 'sysviews'
        {default_order}, 'usage_example_extra': {{'cmd_line_args': 'TODO' }} }}

    def execute(self):
        return self.build()

def main():
    print(report_{name}().execute())
    exit(0)

if __name__ == "__main__":
    main()