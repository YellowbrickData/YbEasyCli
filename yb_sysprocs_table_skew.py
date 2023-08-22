#!/usr/bin/env python3
"""
USAGE:
      yb_table_skew.py [options]

PURPOSE:
      Table skew report.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The report as formatted table, pipe separated value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_table_skew(SPReportUtil):
    """Issue the ybsql commands used to generate a table skew report."""
    config = {
        'description': 'Table skew report.'
        , 'report_sp_location': 'sysviews'
#       , 'report_default_order': 'database|schema|tablename|worker'
        , 'optional_args_multi': ['database', 'schema', 'table', 'worker']
        , 'db_filter_args': {'database':'database_name', 'schema':'schema_name', 'table':'table_name', 'worker':'worker_lid'}
        , 'usage_example_extra': {
            'cmd_line_args': '@$HOME/skew_report.args'
            , 'file_args': [ {'$HOME/skew_report.args': """--skew_pct_column data_skew%
--skew_pct_min 0.005
--report_include_columns \"\"\"
table_id database_name schema_name table_name
distribution worker_count worker_lid worker
unit data_total data_worker data_skew data_skew%
rows_total rows_worker rows_skew rows_skew%
\"\"\" """} ] } }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('arguments')
        args_optional_filter_grp.add_argument("--unit"
            , choices = ('B','K','M','G','T','P')
            , help = "data size unit")
        args_optional_filter_grp.add_argument("--detailed"
            , action="store_true"
            , help = "show detailed report, including all workers")
        pct_columns = ['data_skew%', 'rows_skew%', ]
        args_optional_filter_grp.add_argument("--skew_pct_column"
            , choices = pct_columns
            , help = "limit the report by the selected skew percent column")
        args_optional_filter_grp.add_argument("--skew_pct_min"
            , type = float
            , help = "limit the report by the selected column with the specified minimum percent")

    def additional_args_process(self):
        args = self.args_handler.args
        if (bool(args.skew_pct_column) != bool(args.skew_pct_min)):
            self.args_handler.args_parser.error('both --skew_pct_column and --skew_pct_min must be set')

    def execute(self):
        yb_util_filter = self.db_filter_sql()
        if self.args_handler.args.skew_pct_column:
            yb_util_filter += ' AND %s >= %f' % (
                self.args_handler.args.skew_pct_column
                , self.args_handler.args.skew_pct_min)

        return self.build({
              '_yb_util_filter' : yb_util_filter
            , '_detailed'       : self.args_handler.args.detailed
            , '_unit'           : self.args_handler.args.unit})

def main():
    print(report_table_skew().execute())
    exit(0)

if __name__ == "__main__":
    main()
