#!/usr/bin/env python3
"""
USAGE:
      yb_sysprocs_log_query_pivot.py [options]

PURPOSE:
      Queries for the last week aggregated by hour for use in WLM pivot table analysis.

OPTIONS:
      See the command line help message for all options.
      (yb_sysprocs_log_query_pivot.py --help)

Output:
      The report as a formatted table, pipe separated value rows, or inserted into a database table.
"""
import io, shutil, time, zipfile
from datetime import datetime

#from yb_sp_report_util import SPReportUtil
from yb_common import ArgDate, Common, Report, StoredProc, Text, Util

class report_log_query_pivot(Util):
    """Queries for the last week aggregated by hour for use in WLM pivot table analysis."""
    config = {
        'description': 'Queries for the last week aggregated by hour for use in WLM pivot table analysis.'
        , 'report_sp_location': 'sysviews' }
        #, 'report_default_order': 'yyyy|m|mon|week_begin|date|dow|day|hour|pool|status|app_name|tags|stmt_type|gb_grp|confidence|est_gb_grp|spill' }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def additional_args(self):
        non_su_grp = self.args_handler.args_parser.add_argument_group(
            'non-super database user arguments')
        non_su_grp.add_argument("--non_su", help="non-super database user")

        args_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_grp.add_argument("--from_date", type=ArgDate(), help=("starting DATE(YYYY-MM-DD) "
            "  of statements to analyze, defaults to beginning of previous week (Sunday).") )

        wl_profiler_grp = self.args_handler.args_parser.add_argument_group(
            'log query pivot Excel spreadsheet optional arguments')
        wl_profiler_grp.add_argument("--source_table", default="sys.log_query"
            , help="the source table from where the query metrics are collected, defaults to sys.log_query")
        wl_profiler_grp.add_argument("--close_workbook", action="store_true"
            , help="don't display the Excel spreadsheet, just save the spreadsheet to disk, defaults to FALSE")

        wl_profiler_grp = self.args_handler.args_parser.add_argument_group(
            'log query pivot Excel spreadsheet optional arguments for building spreadsheet in 2 separate steps')
        wl_profiler_grp.add_argument("--step1", action="store_true"
            , help="step 1, retrieve spreadsheet data as CSV data in a single Zip file")
        wl_profiler_grp.add_argument("--step2", metavar="csv_zip_file"
            , help="step 2, build Excel spreadsheet from the provided CSV data")

    def additional_args_process(self):
        if not(self.args_handler.args.step1 or self.args_handler.args.step2):
            self.step1 = self.step2 = True
            self.csv_zip_file = None
        elif self.args_handler.args.step1 and self.args_handler.args.step2:
            Common.error('error: arguments --step1 and --step2, expected one not both arguments')
        elif self.args_handler.args.step1:
            self.step1 = True
            self.step2 = False
            self.csv_zip_file = None
        elif self.args_handler.args.step2:
            self.step1 = False
            self.step2 = True
            self.csv_zip_file = self.args_handler.args.step2
            self.args_handler.args.skip_db_conn = True

        if (self.step1 and not self.args_handler.args.non_su):
            Common.error('error: the following arguments are required: --non_su')
        if (self.step2):
            self.check_xlwings_lib()

    def build_csv_data(self):
        full_proc_name = '{location}_yb{version}/{proc_name}'.format(
            location=self.config['report_sp_location']
            , version=(4 if self.db_conn.ybdb['version_major'] < 5 else 5)
            , proc_name='log_query_pivot_p' )
        sp = StoredProc(full_proc_name)

        args = {}
        if self.args_handler.args.from_date:
              args['_from_ts'] = self.args_handler.args.from_date

        (new_table_name, anonymous_pl) = sp.proc_setof_to_anonymous_block(args)

        tmp_log_query = 'tmp_log_query_%s' % str(time.time()).replace('.', '')
        anonymous_pl = anonymous_pl.replace('sys.log_query', tmp_log_query)

        print('--running %s proc as an anonymous SQL code block' % full_proc_name)
        cmd_result = self.db_conn.ybsql_query("""
SET SESSION AUTHORIZATION {non_su}; -- test that the non-super user exists before continuing
SET SESSION AUTHORIZATION DEFAULT;
CREATE TEMP TABLE {tmp_log_query} AS SELECT * FROM {log_query} DISTRIBUTE RANDOM SORT ON (submit_time);
ALTER TABLE {tmp_log_query} OWNER TO {non_su};
SET SESSION AUTHORIZATION {non_su};
{anonymous_pl};
SELECT * FROM {new_table_name} ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;
""".format(
            non_su=self.args_handler.args.non_su
            , tmp_log_query=tmp_log_query
            , log_query=self.args_handler.args.source_table
            , new_table_name=new_table_name
            , anonymous_pl=anonymous_pl ) )

        cmd_result.on_error_exit()

        if not self.step2:
            zfile = zipfile.ZipFile('%s.zip' % self.pivot_name, 'w', zipfile.ZIP_DEFLATED)
            zinfo = zipfile.ZipInfo('%s.csv' % self.pivot_name)
            zinfo.compress_type = zipfile.ZIP_DEFLATED
            zfile.writestr(zinfo, cmd_result.stdout)
            zfile.close()
            print('--created Zip file: %s.zip' % self.pivot_name)
        else:
            self.report = cmd_result.stdout

    def build_spreadsheet(self):
        if not self.step1:
            zfile = zipfile.ZipFile(self.args_handler.args.step2)
            cfile = zfile.open('%s.csv' % self.pivot_name, 'r')
            cfile = io.TextIOWrapper(cfile, encoding='iso-8859-1', newline='')
            self.report = cfile.read()

            dbv = int((self.args_handler.args.step2.rsplit('.', 1)[0]).split('__')[2][1:])
        else:
            dbv = self.db_conn.ybdb['version_major']

        xlsx_template = ('%s/sql/sysviews_yb%d/log_query_pivot_v%d.xlsx' %
            (Common.util_dir_path, dbv, dbv) )
        filename = '%s.xlsx' % self.pivot_name
        shutil.copyfile(xlsx_template, filename)

        print('--creating Excel file: %s' % filename)
        print('--Excel may present dialogues, reply %s to all dialogues to complete log query pivot spreadsheet'
            % Text.color('positively', style='bold'))

        rowCt = 0
        rows = []
        for line in self.report.split('\n'):
            rowCt += 1
            row = line.split('|')
            if (len(row) > 1):
                rows.append(row)

        import xlwings
        xl_already_running = len(xlwings.apps) > 0
        wb = xlwings.Book(filename)
        sheet = wb.sheets['WLM_PivotData']

        sheet.range('A2:AG10000').delete()
        sheet.range('A2').value = rows

        if Common.is_windows:
            wb.api.ActiveSheet.PivotTables('WorkloadPivot').RefreshTable()
        else:
            wb.api.active_sheet.refresh_all(wb.api)

        wb.save()
        
        if self.args_handler.args.close_workbook:
            if not xl_already_running:
                wb.app.quit()
            else:
                wb.close()
        else:
            wb.activate()
            wb.sheets['WLM_PivotTable'].activate()

    def execute(self):
        if self.step1:
            if not self.db_conn.ybdb['is_super_user']:
                  self.args_handler.args_parser.error("--dbuser '%s' must be a db super user..." % self.db_conn.ybdb['user'])

            non_su_sql = "SELECT COUNT(*) FROM sys.user WHERE name = '%s' AND NOT superuser;" % self.args_handler.args.non_su
            result = self.db_conn.ybsql_query(non_su_sql)
            result.on_error_exit()
            if result.stdout.strip() != '1':
                  self.args_handler.args_parser.error("--non_su '%s' must be a db non-super user..." % self.args_handler.args.non_su)

            self.pivot_name = "yb_log_query_pivot__%s__v%s__%s" % (
                self.db_conn.env['host'].replace('.', '_')
                , self.db_conn.ybdb['version_major'], self.ts)
        elif self.csv_zip_file:
            self.pivot_name = self.csv_zip_file.rsplit('.', 1)[0]

        if self.step1:
            self.build_csv_data()

        if self.step2:
            self.build_spreadsheet()

    def check_xlwings_lib(self):
        try:
            import xlwings
        except Exception as e:
            if str(e) == "No module named 'xlwings'" or str(e) == "No module named xlwings":
                Common.error("the python xlwings library is required, please run 'python -m pip install xlwings' or see https://docs.xlwings.org/en/stable/installation.html for installation instructions")
            else:
                Common.error(e)
            exit(1)

def main():
    report_log_query_pivot().execute()
    exit(0)

if __name__ == "__main__":
    main()