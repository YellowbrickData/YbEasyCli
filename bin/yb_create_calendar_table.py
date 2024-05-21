#!/usr/bin/env python3
"""
USAGE:
      yb_create_calendar_table.py [options]

PURPOSE:
      Create a calendar dimension table.
"""

from datetime import datetime

from yb_common import ArgDate, Common, StoredProc, Util, UtilArgParser

class create_calendar_table(Util):
    config = {
        'description': 'Create a calendar dimension table.' 
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --table us_calendar'
            , 'file_args': [Util.conn_args_file] } }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('calendar arguments')
        args_grp.add_argument("--table", help="name of calendar table to be created, defaults to 'calendar'"
            , default="calendar")
        args_grp.add_argument("--start_date", help="the first date in the calendar, defaults to 1900-01-01"
            , type=ArgDate(), default=datetime.strptime("1900-01-01", "%Y-%m-%d"))
        args_grp.add_argument("--end_date", help="the last date in the calendar, defaults to 2100-12-31"
            , type=ArgDate(), default=datetime.strptime("2100-12-31", "%Y-%m-%d"))
        args_grp.add_argument("--absolute_start_date", help="is an anchor date for the entire calendar table for which several of the calendar columns are measured from, defaults to 1900-01-01"
            , type=ArgDate(), default=datetime.strptime("1900-01-01", "%Y-%m-%d"))

    def additional_args_process(self):
        if self.args_handler.args.start_date > self.args_handler.args.end_date:
            UtilArgParser.error("--start_date(%s) must be less than or equal to --end_date(%s)"
                % (self.args_handler.args.start_date.strftime("%Y-%m-%d")
                    , self.args_handler.args.end_date.strftime("%Y-%m-%d")))

    def execute(self):
        table = Common.quote_object_paths(self.args_handler.args.table)

        print('--Creating calendar table: %s' % table)

        cmd_results = StoredProc('yb_create_calendar_table_p', self.db_conn).call_proc_as_anonymous_block(
            args = {
                'a_table' : table
                , 'a_start_date'          : self.args_handler.args.start_date
                , 'a_end_date'            : self.args_handler.args.end_date
                , 'a_absolute_start_date' : self.args_handler.args.absolute_start_date } )

        cmd_results.on_error_exit()

        print('--Table created')

def main():
    cct = create_calendar_table()
    cct.execute()

if __name__ == "__main__":
    main()