#!/usr/bin/env python3
"""
USAGE:
      yb_create_loopback_remote_server.py [options]

PURPOSE:
      Create a loopback remote server for testing database replication.

OPTIONS:
      See the command line help message for all options.
      (yb_create_dev_db.py --help)

Output:
      TODO.
"""
import re

from yb_common import Common, Text, Util

class CreateLBRemoteServer(Util):
    """Create a loopback remote server for testing database replication.
    """

    config = {
        'description': 'Create a loopback remote server for testing database replication.'
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/create_rules.args --remote_server_name dev_loopback_server'
            , 'file_args': [] }
    }
    dst_schemas = []

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('create loopback remote server arguments')
        args_grp.add_argument("--remote_server_name", help='the name for the remote server being created, defaults to: <host>_loopback', default=None)

    def execute(self):
        if not self.db_conn.ybdb['is_super_user']:
            Common.error("must be run by a super user, '%s' is not a super user..." % (self.db_conn.ybdb['user']))

        sql = 'SHOW SSL SYSTEM;'
        cmd_result = self.db_conn.ybsql_query(sql)
        cmd_result.on_error_exit()

        sql = "IMPORT SSL TRUST FROM '%s';" % cmd_result.stdout.strip()
        cmd_result = self.db_conn.ybsql_query(sql)
        if (cmd_result.exit_code and cmd_result.stderr.strip() != 'ERROR:  This certificate has already been imported for SSL TRUST'):
            cmd_result.on_error_exit()

        sql = 'SHOW SSL CA;'
        cmd_result = self.db_conn.ybsql_query(sql)
        cmd_result.on_error_exit()

        sql = "IMPORT SSL TRUST FROM '%s';" % cmd_result.stdout.strip()
        cmd_result = self.db_conn.ybsql_query(sql)
        if (cmd_result.exit_code and cmd_result.stderr.strip() != 'ERROR:  This certificate has already been imported for SSL TRUST'):
            cmd_result.on_error_exit()

        lb_server_name = (
            self.args_handler.args.remote_server_name
            if self.args_handler.args.remote_server_name
            else ('%s_loopback' % self.db_conn.ybdb['host'].replace('.', '_')) )

        sql = "CREATE REMOTE SERVER %s WITH (HOST 'localhost', NOHOSTNAMECHECK TRUE);" % lb_server_name
        cmd_result = self.db_conn.ybsql_query(sql)
        cmd_result.on_error_exit()
        print('--Created the remote loopback server named: %s' % Text.color(lb_server_name, fg='cyan'))

def main():
    CreateLBRemoteServer().execute()
    exit(0)

if __name__ == "__main__":
    main()