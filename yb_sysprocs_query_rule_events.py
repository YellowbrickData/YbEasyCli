#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_query_rule_events.py [options]

PURPOSE:
      Return the WLM rule events for a query.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_query_rule_events.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_query_rule_events(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Return the WLM rule events for a query.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'event_order'
        , 'usage_example_extra': {'cmd_line_args': "--query_id 12345 --report_exclude query_id" } }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('query event rules arguments')
        args_log_query_grp.add_argument(
            "--query_id", required=True, help=("query_id to report on" ) )
        args_log_query_grp.add_argument(
            "--include_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to include in the report" ) )
        args_log_query_grp.add_argument(
            "--exclude_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to exclude from the report" ) )

    def execute(self):
        where_clause = []
        if self.args_handler.args.include_rule_names:
            where_clause.append('(rule_name IS NULL OR rule_name IN ($$' + ('$$, $$'.join(self.args_handler.args.include_rule_names)) + '$$))')
        if self.args_handler.args.exclude_rule_names:
            where_clause.append('(rule_name IS NULL OR rule_name NOT IN ($$' + ('$$, $$'.join(self.args_handler.args.exclude_rule_names)) + '$$))')
        where_clause = (('(' + (' AND '.join(where_clause)) + ')') if len(where_clause) else None)

        return self.build({'_query_id': self.args_handler.args.query_id}, where_clause)

def main():
    print(report_query_rule_events().execute())
    exit(0)

if __name__ == "__main__":
    main()