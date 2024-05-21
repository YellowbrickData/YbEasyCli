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
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
import time
from yb_common import Common
from yb_sp_report_util import SPReportUtil

class report_query_rule_events(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Return the WLM rule events for a query.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'event_order'
        , 'usage_example_extra': {'cmd_line_args': "--query_id 12345 --report_exclude query_id" } }

    last_query_id = -1

    def additional_args(self):
        args_log_query_grp = self.args_handler.args_parser.add_argument_group('query id arguments')
        args_log_query_grp.add_argument(
            "--query_id", type=int, help="query_id to report on" )

        args_log_query_grp = self.args_handler.args_parser.add_argument_group('last query by DB user name arguments')
        args_log_query_grp.add_argument("--query_user", metavar='USER_NAME'
            , help="last query completed by DB user" )
        args_log_query_grp.add_argument("--poll_x_times", "--poll_X_times"
            , metavar='X', type=int, default=1, help="poll for last query X times, defaults to 1" )
        args_log_query_grp.add_argument("--poll_every_x_secs", "--poll_every_X_secs"
            , metavar='X', type=int, default=5, help="poll every X seconds, defaults to 5" )

        args_log_query_grp = self.args_handler.args_parser.add_argument_group('optional query event rules arguments')
        args_log_query_grp.add_argument(
            "--include_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to include in the report" ) )
        args_log_query_grp.add_argument(
            "--exclude_rule_names", metavar='RULE_NAME', nargs='*', help=("rule names to exclude from the report" ) )
        args_log_query_grp.add_argument(
            "--rule_type",  choices=('compile', 'completion', 'prepare', 'runtime', 'submit')
            , nargs="+", default=None, help="rule types to report, defaults to all rule types")
        args_log_query_grp.add_argument(
            "--event_type",  choices=('debug', 'disabled', 'error', 'ignore', 'info', 'move', 'restart', 'set', 'throttle', 'timeout', 'warn')
            , nargs="+", default=None, help="rule event types to report, defaults to all rule event types")
        args_log_query_grp.add_argument(
            "--print_query", action="store_true", help="print the SQL for the query, defaults to False")

    def additional_args_process(self):
        if (not(bool(self.args_handler.args.query_id) or (self.args_handler.args.query_user))
            or (bool(self.args_handler.args.query_id) and (self.args_handler.args.query_user))):
            self.args_handler.args_parser.error('one of --query_id or the --query_user options must be provided...')

        if (not self.args_handler.args.report_include_columns):
            if (not self.args_handler.args.report_exclude_columns):
                self.args_handler.args.report_exclude_columns = ['query_id']
            elif ('query_id' not in self.args_handler.args.report_exclude_columns):
                self.args_handler.args.report_exclude_columns.append('query_id')

    def process_last_query_run_by(self, is_last):
        user_column = 'username' if (self.db_conn.ybdb['version_major'] > 4) else 'user_name'
        sql = ("""
SELECT NVL(MAX(query_id), -1) AS query_id
FROM sys.log_query
WHERE
type IN ('create table as', 'ctas', 'delete', 'explain', 'insert', 'select', 'update')
AND %s = '%s'
\gset
SELECT NVL(MAX(query_id), -1) FROM sys.query_rule_event WHERE query_id = :query_id"""
            % (user_column, self.args_handler.args.query_user) )
        result = self.db_conn.ybsql_query(sql)
        result.on_error_exit()
        if result.stdout.strip() == '-1':
            message = "No queries found in sys.query_rule_event for DB user '%s'..." % self.args_handler.args.query_user
            if (is_last):
                Common.error(message, color = 'yellow')
            else:
                print(message)

        self.args_handler.args.query_id = int(result.stdout)

    def print_query_rules(self, is_last):
        if self.args_handler.args.query_user:
            self.process_last_query_run_by(is_last)

        if (self.last_query_id != self.args_handler.args.query_id):
            self.last_query_id = self.args_handler.args.query_id

            print('Query Id: %d' % self.args_handler.args.query_id)
            if self.args_handler.args.query_user:
                print('DB User: %s' % self.args_handler.args.query_user)
            if self.args_handler.args.print_query:
                print(self.db_conn.ybsql_query("SELECT query_text FROM sys.log_query WHERE query_id = %d" % self.args_handler.args.query_id).stdout)
            print()

            print(self.build(
                {
                    '_query_id':     self.args_handler.args.query_id
                    , '_rule_type':  self.rule_type
                    , '_event_type': self.event_type }
                , self.where_clause ) )
        elif (self.args_handler.args.query_id != -1):
            print("No new queries found in sys.query_rule_event for DB user '%s'..." % self.args_handler.args.query_user)

    def execute(self):
        self.where_clause = []
        self.rule_type = ''
        self.event_type = ''

        if self.args_handler.args.include_rule_names:
            self.where_clause.append('(rule_name IS NULL OR rule_name IN ($$' + ('$$, $$'.join(self.args_handler.args.include_rule_names)) + '$$))')
        if self.args_handler.args.exclude_rule_names:
            self.where_clause.append('(rule_name IS NULL OR rule_name NOT IN ($$' + ('$$, $$'.join(self.args_handler.args.exclude_rule_names)) + '$$))')
        self.where_clause = (('(' + (' AND '.join(self.where_clause)) + ')') if len(self.where_clause) else None)

        if self.args_handler.args.rule_type:
            self.rule_type = "'" + "', '".join(self.args_handler.args.rule_type) + "'"
        if self.args_handler.args.event_type:
            self.event_type = "'" + "', '".join(self.args_handler.args.event_type) + "'"

        if not self.args_handler.args.query_user:
            self.args_handler.args.poll_x_times = 1

        for i in range(1, self.args_handler.args.poll_x_times + 1):
            is_last = (i == self.args_handler.args.poll_x_times)
            self.print_query_rules(is_last)
            if (not is_last):
                time.sleep(self.args_handler.args.poll_every_x_secs)

def main():
    report_query_rule_events().execute()
    exit(0)

if __name__ == "__main__":
    main()