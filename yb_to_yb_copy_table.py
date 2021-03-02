#!/usr/bin/env python3
"""
USAGE:
      yb_to_yb_copy_tables.py [options]

PURPOSE:
      Copy table/s from source cluster to destination cluster.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      Tables that have been copied.
"""

import sys
import os
import re
import random
from datetime import datetime

from yb_common import ArgsHandler, Cmd, Common, DBConnect, DBFilterArgs, IntRange, Text, Util
from yb_chunk_dml_by_integer import chunk_dml_by_integer

class yb_to_yb_copy_table(Util):
    """Issue the command used to list the table names found in a particular
    database.
    """
    config = {
        'description': (
            'Copy a table from a source cluster to a destination cluster.'
            '\n'
            '\nnote:'
            '\n  If the src and dst user password differ use SRC_YBPASSWORD and DST_YBPASSWORD env variables.'
            '\n  For manual password entry unset all env passwords or use the --src_W and --dst_W options.')
        , 'positional_args_usage': None
        #, 'output_tmplt_vars': ['ddl']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --unload_where_clause 'sale_id BETWEEN 1000 AND 2000' --src_table Prod.sales --dst_table dev.sales --log_dir tmp --"
            , 'file_args': [ {'$HOME/conn.args': """--src_host yb14
--src_dbuser dze
--src_conn_db stores_prod
--dst_host yb89
--dst_dbuser dze
--dst_conn_db stores_dev"""} ] } }

    def init(self, src_conn=None, dst_conn=None, args_handler=None):
        """Initialize yb_to_yb_copy_tables class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if src_conn:
            self.src_conn = src_conn
            self.dst_conn = dst_conn
            self.args_handler = args_handler
        else:
            self.args_handler = ArgsHandler(self.config, init_default=False)

            self.add_args()

            self.args_handler.args_process()

    def add_args(self):
        self.args_handler.args_process_init()
        self.args_handler.args_add_optional()
        self.args_handler.args_add_connection_group('src', 'source')
        self.args_handler.args_add_connection_group('dst', 'destination')
        self.args_handler.args_usage_example()

        copy_table_r_grp = self.args_handler.args_parser.add_argument_group('required copy table arguments')
        copy_table_r_grp.add_argument(
            "--src_table", required=True
            , help=("source table to copy"))
        copy_table_r_grp.add_argument(
            "--dst_table", required=True
            , help=("destination table"))

        copy_table_o_grp = self.args_handler.args_parser.add_argument_group('optional copy table arguments')
        copy_table_o_grp.add_argument(
            "--log_prefix", help=("prefix placed on log files"))
        copy_table_o_grp.add_argument(
            "--log_dir", help=("directory where log files will be created"))
        copy_table_o_grp.add_argument(
            "--delimiter", default="0x1f" 
            , help=("column delimiter used by unload and load, defaults to '0x1f'"))
        copy_table_o_grp.add_argument(
            "--ybunload_options", help=("additional ybunload options"))
        copy_table_o_grp.add_argument(
            "--ybload_options", help=("additional ybload options"))
        copy_table_o_grp.add_argument(
            "--unload_where_clause", dest='where_clause'
            , help=("SQL clause which filters the rows to copy from the source table"))
        copy_table_o_grp.add_argument("--create_dst_table", action="store_true"
            , help="create destination table from source table ddl before copying table")
        copy_table_o_grp.add_argument(
            "--chunk_rows", dest="chunk_rows", metavar='ROWS'
            , type=IntRange(1,9223372036854775807)
            , help="when set data copying will be performed in chunks of rows rather than one big copy")
        copy_table_o_grp.add_argument(
            "--threads"
            , type=IntRange(1,20), default=1
            , help="when set data copying will be performed in parallel ybunload/ybload threads")

    def set_db_connections(self):
        pwd = os.environ['YBPASSWORD'] if 'YBPASSWORD' in os.environ else None
        src_pwd = os.environ['SRC_YBPASSWORD'] if 'SRC_YBPASSWORD' in os.environ else None
        if src_pwd:
            os.environ['YBPASSWORD'] = src_pwd
        self.src_conn = DBConnect(args_handler=self.args_handler, conn_type='src')
        if pwd:
            os.environ['YBPASSWORD'] = pwd
        elif src_pwd:
            del os.environ['YBPASSWORD']

        dst_pwd = os.environ['DST_YBPASSWORD'] if 'DST_YBPASSWORD' in os.environ else None
        if dst_pwd:
            os.environ['YBPASSWORD'] = dst_pwd
        self.dst_conn = DBConnect(args_handler=self.args_handler, conn_type='dst')
        if pwd:
            os.environ['YBPASSWORD'] = pwd
        elif dst_pwd:
            del os.environ['YBPASSWORD']

    def additional_args_process(self):
        if self.args_handler.args.create_dst_table:
            self.src_to_dst_table_ddl(
                self.args_handler.args.src_table, self.args_handler.args.dst_table
                , self.src_conn, self.dst_conn
                , self.args_handler)
            print('-- created destination table: %s' % self.args_handler.args.dst_table)

        #thread use may have severe impact on the YB cluster, so I'm limiting it to super users
        if (self.args_handler.args.threads > 1
            and not self.src_conn.ybdb['is_super_user']
            and not self.dst_conn.ybdb['is_super_user']):
            Common.error(Text.color(
                "The '--threads' option is only supported for YBDB super users."
                , 'yellow'))

        if (self.args_handler.args.chunk_rows
            and self.src_conn.ybdb['version_major'] < 4):
            Common.error(Text.color(
                "The '--chunk_rows' option is only supported on YBDB version 4 or higher."
                " The source db is running YBDB %s..." % self.src_conn.ybdb['version']
                , 'yellow'))
            
        self.log_file_name_template = ('{}{}{}_{}_{{{{CofC}}}}{{{{TofT}}}}_{{log_type}}.log'.format(
            ('%s/' % self.args_handler.args.log_dir
                if self.args_handler.args.log_dir
                else '')
            , ('%s_' % self.args_handler.args.log_prefix
                if self.args_handler.args.log_prefix
                else '')
            , datetime.now().strftime("%Y%m%d_%H%M%S")
            , "%04d" % random.randint(0,9999)))

    def build_table_copy_cmd(self):
        ybunload_env = "YBPASSWORD=$SRC_YBPASSWORD"
        ybunload_cmd = ("ybunload"
            " -h {src_host}"
            "{port_option}"
            " -U {src_user}"
            " -d {src_db}"
            " --delimiter '{delimiter}'"
            " --stdout true"
            " --quiet true"
            " --logfile {log_file_name}"
            "{additionl_options}"
            """ --select "{{unload_sql}}" """).format(
            src_host = self.src_conn.env['host']
            , port_option = (' --port %s' % self.args_handler.args.src_port if self.args_handler.args.src_port else '')
            , src_user = self.src_conn.env['dbuser']
            , src_db = self.src_conn.database
            , delimiter = self.args_handler.args.delimiter
            , log_file_name = (self.log_file_name_template.format(log_type='ybunload'))
            , additionl_options = (' %s' % self.args_handler.args.ybunload_options if self.args_handler.args.ybunload_options else ''))

        if (self.args_handler.args.ybload_options
            and re.search('logfile-log-level', self.args_handler.args.ybload_options, re.IGNORECASE)):
            #the user has set their own log level in ybload_options
            logfile_log_level_option = ''
        else:
            #set default log level 
            logfile_log_level_option = ' --logfile-log-level INFO'

        ybload_env = "YBPASSWORD=$DST_YBPASSWORD"
        ybload_cmd = ("ybload"
            " -h {dst_host}"
            "{port_option}"
            " -U {dst_user}"
            " -d {dst_db}"
            " -t '{dst_table}'"
            " --delimiter '{delimiter}'"
            " --log-level OFF" #turns off console logging
            "{logfile_log_level_option}"
            " --logfile {log_file_name}"
            " --bad-row-file {bad_log_file_name}"
            "{additionl_options}"
            " -- -").format(
            dst_host = self.dst_conn.env['host']
            , port_option = (' --port %s' % self.args_handler.args.dst_port if self.args_handler.args.dst_port else '')
            , dst_user = self.dst_conn.env['dbuser']
            , dst_db = self.dst_conn.database
            , dst_table = Common.quote_object_paths(self.args_handler.args.dst_table)
            , delimiter = self.args_handler.args.delimiter
            , log_file_name = (self.log_file_name_template.format(log_type='ybload'))
            , logfile_log_level_option = logfile_log_level_option
            , bad_log_file_name = (self.log_file_name_template.format(log_type='ybload_bad'))
            , additionl_options = (' %s' % self.args_handler.args.ybload_options if self.args_handler.args.ybload_options else ''))

        self.table_copy_cmd = ("{ybunload_env} {ybunload_cmd}"
            " | {ybload_env} {ybload_cmd}").format(
            ybunload_env = ybunload_env
            , ybunload_cmd = ybunload_cmd
            , ybload_env = ybload_env
            , ybload_cmd = ybload_cmd)

    def chunk_table_unload_sql(self, table_unload_sql):
        self.args_handler.args.dml = ("%s AND <chunk_where_clause>" % table_unload_sql)
        self.args_handler.args.execute_chunk_dml = False
        self.args_handler.args.verbose_chunk_off = False
        self.args_handler.args.null_chunk_off = False
        self.args_handler.args.print_chunk_dml = True
        self.args_handler.args.table = Common.quote_object_paths(self.args_handler.args.src_table)
        self.args_handler.args.column = 'rowunique'
        self.args_handler.args.column_cardinality = 'high'
        if self.args_handler.args.where_clause:
            self.args_handler.args.table_where_clause = self.args_handler.args.where_clause
        else:
            self.args_handler.args.table_where_clause = 'TRUE'

        cdml = chunk_dml_by_integer(db_conn=self.src_conn, args_handler=self.args_handler)
        cdml.execute()
        if cdml.cmd_results.exit_code:
            cdml.cmd_results.write()
            exit(cdml.cmd_results.exit_code)

        return cdml.cmd_results.stdout.strip().split('\n')

    def execute(self):
        table_unload_sql = "SELECT * FROM {src_table} WHERE TRUE{where_clause}".format(
            src_table = Common.quote_object_paths(self.args_handler.args.src_table).replace('"','"\\""')
            , where_clause=(' AND %s' % self.args_handler.args.where_clause if self.args_handler.args.where_clause else ''))

        if self.args_handler.args.chunk_rows:
            chunks_sql = self.chunk_table_unload_sql(table_unload_sql)
        else:
            chunks_sql = [table_unload_sql]

        os.environ['SRC_YBPASSWORD'] = self.src_conn.env['pwd']
        os.environ['DST_YBPASSWORD'] = self.dst_conn.env['pwd']

        total_chunks = len(chunks_sql)
        format_CofC = 'chunk%.0{len}dof%.0{len}d'.format(len=len(str(total_chunks)))
        total_threads = self.args_handler.args.threads
        format_TofT = '_thread%.0{len}dof%.0{len}d'.format(len=len(str(total_threads)))
        TofT = ''
        thread_clause = ''
        for chunk in range(1,total_chunks+1):
            unload_sql = chunks_sql[chunk-1]
            CofC = format_CofC % (chunk, total_chunks)

            cmd_threads = []
            for thread in range(1,total_threads+1):
                if total_threads > 1:
                    thread_clause = ' AND /* thread_clause(thread: %d) >>>*/ rowunique %% %d = %d /*<<< thread_clause */' % (thread, total_threads, thread-1)
                    TofT = format_TofT % (thread, total_threads)
                copy_cmd = self.table_copy_cmd.format(
                    unload_sql=unload_sql.rstrip().rstrip(';') + thread_clause
                    , CofC=CofC
                    , TofT=TofT)
                cmd = Cmd(copy_cmd, False, wait=False)
                cmd.TofT = TofT
                cmd_threads.append(cmd)

            thread_exit_code = 0
            thread_failed = False
            for cmd in cmd_threads:
                cmd.wait()

                loaded = False
                print('-- %s%s'
                    % (CofC, cmd.TofT))
                if cmd.exit_code == 0:
                    ybload_log_file_name = self.log_file_name_template.format(
                        log_type='ybload').format(
                            CofC=CofC
                            , TofT=cmd.TofT)
                    file = open(ybload_log_file_name, "r")
                    for line in file:
                        if re.search('SUCCESSFUL BULK LOAD', line):
                            loaded = True
                            sys.stdout.write(line)
                            break

                if not loaded:
                    cmd.write()
                    log_file_name = self.log_file_name_template.format(
                        log_type='*').format(
                            CofC=CofC
                            , TofT=cmd.TofT)
                    print('Table Copy {}, please review the log files: {}'.format(
                        Text.color('Failed', 'red'), log_file_name))
                    thread_exit_code = cmd.exit_code
                    thread_failed = True

            if thread_failed:
                exit(thread_exit_code)

        del os.environ['SRC_YBPASSWORD']
        del os.environ['DST_YBPASSWORD']

        exit(0)

def main():
    ytoy = yb_to_yb_copy_table(init_default=False)
    ytoy.init()

    ytoy.set_db_connections()

    ytoy.additional_args_process()

    ytoy.build_table_copy_cmd()

    ytoy.execute()


if __name__ == "__main__":
    main()