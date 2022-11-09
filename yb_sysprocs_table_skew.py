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
      The report as formatted table, pipe seperted value rows, or inserted into a database table.
"""
from yb_sp_report_util import SPReportUtil

class report_table_skew(SPReportUtil):
    """Issue the ybsql commands used to generate a table skew report."""
    config = {
        'description': 'Table skew report.'
        , 'report_sp_location': 'sysviews'
        , 'report_default_order': 'owner|database|schema|tablename'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'table']
        , 'db_filter_args': {'owner':'owner', 'schema':'schema', 'table':'tablename', 'database':'database'}
        , 'usage_example_extra': {
            'cmd_line_args': '@$HOME/skew_report.args'
            , 'file_args': [ {'$HOME/skew_report.args': """--skew_pct_column disk_skew_max_pct_of_wrkr
--skew_pct_min 0.005
--report_include_columns \"\"\"
owner database schema table
disk_skew_max_pct_of_wrkr cmprs_ratio
rows_total
gbytes_total
gbytes_wrkr_min gbytes_wrkr_max
gbytes_minus_parity gbytes_total_uncmprs
\"\"\" """} ] } }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        pct_columns = [
            'disk_skew_max_pct_of_wrkr', 'disk_skew_avg_pct_of_wrkr'
            , 'disk_skew_max_pct_of_tbl', 'disk_skew_avg_pct_of_tbl'
            , 'row_skew_max_pct_of_tbl', 'row_skew_avg_pct_of_tbl']
        args_optional_filter_grp.add_argument("--skew_pct_column"
            , choices = pct_columns
            , help="limit the report by the selected skew percent column")
        args_optional_filter_grp.add_argument("--skew_pct_min"
            , type=float
            , help="limit the report by the selected column with the specified minimum percent")

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

        return self.build({'_yb_util_filter' : yb_util_filter })

def main():
    print(report_table_skew().execute())
    exit(0)

if __name__ == "__main__":
    main()
