#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_column_dstr.py [options]

PURPOSE:
      Distribution of rows per distinct values for column grouped on a logarithmic scale.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_column_dstr.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_column_dstr(SPReportUtil):
    """Issue the ybsql commands used to create the column distribution report."""
    config = {
        'description': 'Distribution of rows per distinct values for column grouped on a logarithmic scale.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'column_name|rows_per'
        , 'required_args_single': ['schema', 'table', 'column']
        , 'db_filter_args': {'database':'db_name', 'schema':'table_schema', 'table':'table_name'}
        , 'usage_example_extra': {'cmd_line_args': "--schema dev --table store --column invoice_total" } }

    def additional_args(self):
        args_column_dstr_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_column_dstr_grp.add_argument(
            "--log_n", type=int, default=10
            , help="use log and powers of n; defaults to 10")

    def execute(self):
        return self.build({
            '_db_name'       : self.db_conn.database
            , '_schema_name' : self.args_handler.args.schema
            , '_table_name'  : self.args_handler.args.table
            , '_column_name' : self.args_handler.args.column
            , '_log_n'       : self.args_handler.args.log_n })

def main():
    print(report_column_dstr().execute())
    exit(0)

if __name__ == "__main__":
    main()