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
            '\n  You can also create the heatmap manually by following the README steps in sql/wl_profiler_ybX.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args'
            , 'file_args': [Util.conn_args_file] } }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def additional_args(self):
        su_connection_grp = self.args_handler.args_parser.add_argument_group(
            'super user connection arguments')
        su_connection_grp.add_argument("--su_dbuser"
            , help="su database user, overrides SU_YBUSER env variable")
        su_connection_grp.add_argument("--su_W", action="store_true"
            , help="prompt for password instead of using the SU_YBPASSWORD env variable")

        wl_profiler_grp = self.args_handler.args_parser.add_argument_group(
            'wl profiler heatmap optional arguments')
        wl_profiler_grp.add_argument("--keep_db_objects", action="store_false"
            , help="do not delete the temporary db object, defaults to FALSE")
        wl_profiler_grp.add_argument("--close_workbook", action="store_true"
            , help="don't display the heatmap, just save the spreadsheet to disk, defaults to FALSE")

        wl_profiler_grp = self.args_handler.args_parser.add_argument_group(
            'wl profiler heatmap optional arguments for building heatmap in 2 separate steps')
        wl_profiler_grp.add_argument("--step1", action="store_true"
            , help="step 1, retrieve heatmap data as CSV data in a single Zip file")
        wl_profiler_grp.add_argument("--step2", metavar="csv_zip_file"
            , help="step 2, build heatmap Excel from the provided CSV data")

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

        if self.step2:
            try:
                import xlwings
            except Exception as e:
                if str(e) == "No module named 'xlwings'":
                    Common.error("the python xlwings library is required, please run 'python -m pip install xlwings' or see https://docs.xlwings.org/en/stable/installation.html for installation instructions")
                else:
                    Common.error(e)
                exit(1)

    def init(self):
        if self.step1:
            self.complete_db_conn()
            self.wlp_version = (4 if self.db_conn.ybdb['version_major'] <= 4 else 5)
            self.profile_name = "yb_wl_profile__%s__v%s__%s" % (self.db_conn.env['host'].replace('.', '_'), self.wlp_version, self.ts)
        elif self.csv_zip_file:
            self.profile_name = self.csv_zip_file.rsplit('.', 1)[0]
            self.wlp_version = int(self.profile_name.split('__')[2][1:])

        print('--creating temp directory: %s' % self.profile_name)
        os.mkdir(self.profile_name)
        os.chdir(self.profile_name)

        if self.csv_zip_file:
            shutil.unpack_archive('../%s' % self.csv_zip_file, '.', 'zip')

    def complete_db_conn(self):
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

    def build_csv_data(self):
        self.run_sql()
        if not self.step2:
            shutil.make_archive('../%s' % self.profile_name, 'zip', '../%s' % self.profile_name)
            print('--created Zip file: %s.zip' % self.profile_name)

    def build_heatmap(self):
        xlsm_template = ('%s/sql/wl_profiler_yb%d/wl_profile.xlsm' %
                (Common.util_dir_path, self.wlp_version) )
        self.filename = '%s.xlsm' % self.profile_name
        print('--creating Excel file: %s' % self.filename)
        self.filename = '../%s' % self.filename
        print('--Excel may present dialogues, reply %s to all dialogues to complete WL profile spreadsheet'
            % Text.color('positively', style='bold'))
        shutil.copyfile(xlsm_template, self.filename)
        sheets = ['Data', 'Totals_User', 'Totals_App', 'Totals_Pool', 'Totals_Step']

        import xlwings
        xl_already_running = len(xlwings.apps) > 0
        wb = xlwings.Book(self.filename)
        for sheet_name in sheets:
            file_suffix = sheet_name.split('_')[-1].lower()
            sheet = wb.sheets[sheet_name]

            with open('wl_profiler_%s.csv' % (file_suffix)) as csv_file:
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
        self.init()

        if self.step1:
            self.build_csv_data()

        if self.step2:
            self.build_heatmap()

        print('--droping temp directory: %s' % self.profile_name)
        os.chdir('..')
        shutil.rmtree(self.profile_name)


def main():
    wl_profiler().execute()


if __name__ == "__main__":
    main()
