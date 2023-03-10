#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_slot_usage.py [options]

PURPOSE:
      Create a WLM slot usage report by analyzing sys.log_query data.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_slot_usage.py --help)

Output:
      The report as a formatted table, pipe seperated value rows, or inserted into a database table.
"""

from datetime import date, timedelta
from yb_common import ArgDate, ArgIntRange
from yb_sp_report_util import SPReportUtil

class report_log_query_slot_usage(SPReportUtil):
    """Issue the ybsql commands used to completed backend statements report."""
    config = {
        'description': (
            'Create a WLM slot usage report by analyzing sys.log_query data.'
            '\n'
            '\nnote:'
            '\n  This Utility must be run as a super user and requires a second '
            '\n  non-super DB user(--non_su) to perform analytic SQL queries.')
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'pool_id|slots'
        , 'usage_example_extra': {'cmd_line_args': "--non_su dze --days_of_week MO TU WE TH FR --hours_of_day 9 10 11 12 13 14 15 16" } }

    def additional_args(self):
        non_su_grp = self.args_handler.args_parser.add_argument_group(
            'non-super database user argument')
        non_su_grp.add_argument("--non_su", required=True, help="non-super database user")

        args_report_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_report_grp.add_argument(
            "--days" , type=ArgIntRange(1,365), default=30
            , help="number of sys.log_query days to analyze, defaults to: 30")
        args_report_grp.add_argument(
            "--days_of_week" , choices=['SU', 'MO', 'TU', 'WE', 'TH', 'FR', 'SA'], nargs='+'
            , help="days of the week to report on, defaults to all days of the week")
        args_report_grp.add_argument(
            "--hours_of_day" , type=ArgIntRange(0,23), nargs='+'
            , help="hours of the day(0-23) to report on, defaults to all hours of the day")
        args_report_grp.add_argument("--from_date", type=ArgDate(), help=("starting DATE(YYYY-MM-DD) "
            "of sys.log_query to analyze, defaults to DAYS argument days before today.") )

    def additional_args_process(self):
        if not self.args_handler.args.from_date:
            self.args_handler.args.from_date = date.today() - (timedelta(self.args_handler.args.days - 1))

        self.report_header = ('--report from: %s, to: %s' % (
            self.args_handler.args.from_date.strftime("%Y-%m-%d")
            , (self.args_handler.args.from_date + timedelta(self.args_handler.args.days - 1)).strftime("%Y-%m-%d") ) )

        if self.args_handler.args.days_of_week:
            self.report_header += ('\n--days of the week: %s' % ', '.join(self.args_handler.args.days_of_week)) 
            day_to_int = {'SU':0, 'MO':1, 'TU':2, 'WE':3, 'TH':4, 'FR':5, 'SA':6}
            self.args_handler.args.days_of_week = ", ".join([str(day_to_int[d]) for d in self.args_handler.args.days_of_week])
        else:
            self.args_handler.args.days_of_week = ''

        if self.args_handler.args.hours_of_day:
            hours_of_day = ", ".join([str(i) for i in self.args_handler.args.hours_of_day])
            self.report_header += ('\n--hours of the day: %s' % hours_of_day)
            self.args_handler.args.hours_of_day = hours_of_day
        else:
            self.args_handler.args.hours_of_day = ''

    def execute(self):
        args = {
            '_non_su': self.args_handler.args.non_su
            , '_days': self.args_handler.args.days
            , '_from_date': self.args_handler.args.from_date
            , '_days_of_week': self.args_handler.args.days_of_week
            , '_hours_of_day': self.args_handler.args.hours_of_day }
        print(self.report_header)
        print(self.build(args))

def main():
    report_log_query_slot_usage().execute()
    exit(0)

if __name__ == "__main__":
    main()