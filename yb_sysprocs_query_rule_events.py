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
from yb_common import Common
from yb_sp_report_util import SPReportUtil

class report_query_rule_events(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Return the WLM rule events for a query.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'event_order'
        , 'usage_example_extra': {'cmd_line_args': "--query_id 12345 --report_exclude query_id" } }

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('required query event rules arguments')
        args_log_query_grp.add_argument(
            "--query_id", type=int, help=("query_id to report on" ) )
        args_log_query_grp.add_argument(
            "--last_query_run_by", metavar='USER_NAME', help=("last query completed by DB user" ) )

        args_log_query_grp = self.args_handler.args_parser.add_argument_group('optional query event rules arguments')
        args_log_query_grp.add_argument(
            "--include_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to include in the report" ) )
        args_log_query_grp.add_argument(
            "--exclude_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to exclude from the report" ) )
        args_log_query_grp.add_argument(
            "--rule_type",  choices=('compile', 'completion', 'prepare', 'runtime', 'submit')
            , nargs="+", default=None, help="rule types to report, defaults to all rule types")
        args_log_query_grp.add_argument(
            "--event_type",  choices=('disabled', 'error', 'ignore', 'info', 'move', 'set', 'timeout', 'warn')
            , nargs="+", default=None, help="rule event types to report, defaults to all rule event types")
        args_log_query_grp.add_argument(
            "--print_query", action="store_true", help="print the SQL for the query, defaults to False")

    def additional_args_process(self):
        if (not(bool(self.args_handler.args.query_id) or (self.args_handler.args.last_query_run_by))
            or (bool(self.args_handler.args.query_id) and (self.args_handler.args.last_query_run_by))):
            self.args_handler.args_parser.error('one of --query_id or the --last_query_run_by options must be provided...')

        if (not self.args_handler.args.report_include_columns):
            if (not self.args_handler.args.report_exclude_columns):
                self.args_handler.args.report_exclude_columns = ['query_id']
            elif ('query_id' not in self.args_handler.args.report_exclude_columns):
                self.args_handler.args.report_exclude_columns.append('query_id')

    def process_last_query_run_by(self):
        if self.args_handler.args.last_query_run_by:
            user_column = 'username' if (self.db_conn.ybdb['version_major'] > 4) else 'user_name'
            sql = ("""
SELECT MAX(query_id) AS query_id
FROM sys.log_query
WHERE
    type IN ('create table as', 'ctas', 'delete', 'explain', 'insert', 'select', 'update')
    AND %s = '%s'
\gset
SELECT DISTINCT query_id FROM sys.query_rule_event WHERE query_id = :query_id"""
                % (user_column, self.args_handler.args.last_query_run_by) )
            result = self.db_conn.ybsql_query(sql)
            result.on_error_exit()
            if result.stdout.strip() == '':
                Common.error("No queries found in sys.query_rule_event for DB user '%s'..." % self.args_handler.args.last_query_run_by, color = 'yellow')
            else:
                self.args_handler.args.query_id = int(result.stdout) 

    def execute(self):
        self.process_last_query_run_by()

        where_clause = []
        rule_type = ''
        event_type = ''

        if self.args_handler.args.include_rule_names:
            where_clause.append('(rule_name IS NULL OR rule_name IN ($$' + ('$$, $$'.join(self.args_handler.args.include_rule_names)) + '$$))')
        if self.args_handler.args.exclude_rule_names:
            where_clause.append('(rule_name IS NULL OR rule_name NOT IN ($$' + ('$$, $$'.join(self.args_handler.args.exclude_rule_names)) + '$$))')
        where_clause = (('(' + (' AND '.join(where_clause)) + ')') if len(where_clause) else None)

        if self.args_handler.args.rule_type:
            rule_type = "'" + "', '".join(self.args_handler.args.rule_type) + "'"
        if self.args_handler.args.event_type:
            event_type = "'" + "', '".join(self.args_handler.args.event_type) + "'"

        print('Query Id: %d' % self.args_handler.args.query_id)
        if self.args_handler.args.last_query_run_by:
            print('DB User: %s' % self.args_handler.args.last_query_run_by)
        if self.args_handler.args.print_query:
            print(self.db_conn.ybsql_query("SELECT query_text FROM sys.log_query WHERE query_id = %d" % self.args_handler.args.query_id).stdout)
        print()

        return self.build(
            {
                '_query_id':     self.args_handler.args.query_id
                , '_rule_type':  rule_type
                , '_event_type': event_type }
            , where_clause )

def main():
    print(report_query_rule_events().execute())
    exit(0)

if __name__ == "__main__":
    main()