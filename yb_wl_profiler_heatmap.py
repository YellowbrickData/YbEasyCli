#!/usr/bin/env python3
"""
TODO
USAGE:
      yb_wl_profiler_heatmap.py [options]

PURPOSE:
      Creates a 35 day Excel heatmap of Work Loads on a Yellowbrick Cluster.

OPTIONS:
      See the command line help message for all options.
      (yb_wl_profiler_heatmap.py --help)

Output:
      Excel heatmap
"""
import csv
import os
import shutil
from datetime import datetime

from yb_common import Common, DBConnect, Text, Util

def floatIfFloat(str):
    try:
        return float(str)
    except ValueError:
        return str

class wl_profiler(Util):
    """Creates a 35 day Excel heatmap of Work Loads on a Yellowbrick Cluster.
    """
    config = {
        'description': (
            'Creates a 35 day Excel heatmap of Work Loads on a Yellowbrick Cluster.'
            '\n'
            '\nnote:'
            '\n  This utility requires 2 db user logins, a non-super-user to create temporary db objects and a'
            '\n  db super-user to read the system views.'
            '\n  You can also create the heatmap manualy by following the README steps in sql/wl_profiler_ybX.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args'
            , 'file_args': [Util.conn_args_file] } }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def test_prerequisites():
        try:
            import xlwings
        except Exception as e:
            if str(e) == "No module named 'xlwings'":
                Common.error("the python xlwings library is required, please run 'python -m pip install xlwings' or see https://docs.xlwings.org/en/stable/installation.html for installation instructions")
            else:
                Common.error(e)
            exit(1)

    def additional_args(self):
        su_connection_grp = self.args_handler.args_parser.add_argument_group(
            'super user connection arguments')
        su_connection_grp.add_argument("--su_dbuser"
            , help="su database user, overrides SU_YBUSER env variable")
        su_connection_grp.add_argument("--su_W", action="store_true"
            , help="prompt for password instead of using the SU_YBPASSWORD env variable")

        wl_profiler_grp = self.args_handler.args_parser.add_argument_group(
            'wl profiler heatmap argument')
        wl_profiler_grp.add_argument("--keep_db_objects", action="store_false"
            , help="do not delete the temporary db object, defaults to FALSE")
        wl_profiler_grp.add_argument("--close_workbook", action="store_true"
            , help="run in batch mode - don't display the HeatMap Workbook, just silently save it to disk")

    def complete_db_conn(self):
        self.wlp_version = (4 if self.db_conn.ybdb['version_major'] <= 4 else 5)

        if self.db_conn.ybdb['is_super_user']:
            self.args_handler.args_parser.error("dbuser '%s' must not ba a db super user..." % self.db_conn.ybdb['user'])

        su_env = self.db_conn.env.copy()

        su_env['conn_db'] = self.db_conn.database
        su_env['dbuser'] = (
            self.args_handler.args.su_dbuser
            if self.args_handler.args.su_dbuser
            else os.getenv('SU_YBUSER') )
        su_env['pwd'] = (
            None
            if self.args_handler.args.su_W
            else os.getenv('SU_YBPASSWORD') )

        if not su_env['dbuser']:
            self.args_handler.args_parser.error("the su database user must be set using the SU_YBUSER environment variable or with the argument: --su_dbuser")
        else:
            DBConnect.set_env(su_env)
            self.su_db_conn = DBConnect(env=su_env, conn_type='su')
            DBConnect.set_env(self.db_conn.env_pre)

            if not self.su_db_conn.ybdb['is_super_user']:
                self.args_handler.args_parser.error("su_dbuser '%s' is not a db super user..." % su_env['dbuser'])

    def run_sql(self):
        sql_scripts = [
            {'file': 'step0_wl_profiler_drop_objects.sql', 'conn': self.db_conn}
            , {'file': 'step1_wl_profiler_create_objects.sql', 'conn': self.db_conn}
            , {'file': 'step2_wl_profiler_su_populate.sql', 'conn': self.su_db_conn, 'schema': self.db_conn.schema}
            , {'file': 'step3_wl_profiler_populate.sql', 'conn': self.db_conn}
            , {'file': 'step4_wl_profiler_create_csv_files.sql', 'conn': self.db_conn} ]

        if self.args_handler.args.keep_db_objects:
            sql_scripts.append({'file': 'step0_wl_profiler_drop_objects.sql', 'conn': self.db_conn})

        for script in sql_scripts:
            filename = ('%s/sql/wl_profiler_yb%d/%s' %
                (Common.util_dir_path, self.wlp_version, script['file']) )
            print('--db user: %s, executing: %s' % (script['conn'].ybdb['user'], filename))
            sql = open(filename).read()
            if 'schema' in script:
                sql = "SET SCHEMA '%s';\n%s" % (script['schema'], sql)
            result = script['conn'].ybsql_query(sql)
            result.on_error_exit()

    def load_csv_to_xls(self):
        xlsm_template = ('%s/sql/wl_profiler_yb%d/wl_profile.xlsm' %
                (Common.util_dir_path, self.wlp_version) )
        print('--creating Excel file: %s' % self.filename)
        print('--Excel may present dialogues, reply %s to all dialogues to complete WL profile spreadsheet'
            % Text.color('positively', style='bold'))
        shutil.copyfile(xlsm_template, '../' + self.filename)
        sheets = ['Data', 'Totals_User', 'Totals_App', 'Totals_Pool', 'Totals_Step']

        import xlwings
        xl_already_running = len(xlwings.apps) > 0
        wb = xlwings.Book('../' + self.filename)
        for sheet_name in sheets:
            file_suffix = sheet_name.split('_')[-1].lower()
            sheet = wb.sheets[sheet_name]

            with open('wl_profiler_%s.csv' % file_suffix) as csv_file:
                rows = []
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    rows.append(row)
                sheet.range('A2').value = rows
            csv_file.close()
        wb.save()
        if self.args_handler.args.close_workbook:
            if not xl_already_running:
                wb.app.quit()
            else:
                wb.close()
        else:
            wb.activate()

    def execute(self):
        self.complete_db_conn()

        self.profile_name = "yb_wl_profile__%s__%s" % (self.db_conn.env['host'].replace('.', '_'), self.ts)
        self.filename = '%s.xlsm' % self.profile_name
        print('--creating temp directory: %s' % self.profile_name)
        os.mkdir(self.profile_name)
        os.chdir(self.profile_name)

        self.run_sql()
        self.load_csv_to_xls()

        print('--created Excel file: %s' % self.filename)
        print('--droping temp directory: %s' % self.profile_name)
        os.chdir('..')
        shutil.rmtree(self.profile_name)

def main():
    wl_profiler.test_prerequisites()

    wlp = wl_profiler()

    wlp.execute()


if __name__ == "__main__":
    main()
