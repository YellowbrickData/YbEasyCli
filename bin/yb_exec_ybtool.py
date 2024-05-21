#!/usr/bin/env python3
"""
USAGE:
      yb_exec_ybtool.py [options]

PURPOSE:
      Execute a ybtool with a unified DB connection arguments/method.

OPTIONS:
      See the command line help message for all options.
      (yb_exec_ybtool.py --help)

Output:
      Output of ybtool being run.
"""
import argparse
import sys

from yb_common import Util

class ExecYBTool(Util):
    """Execute a ybtool with a unified DB connection arguments/method.
    """
    config = {
        'description': (
            'Execute a ybtool with a unified DB connection arguments/method.'
            '\n'
            '\nnote:'
            '\n  Mainly used to provide a unified DB login method for all ybtools. '
            '\n  With yb_exec_ybtool.py all ybtools can perform DB login with a .ybpass file.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --ybtool_cmd ybunload --table stores --stdout --logfile unload.log --quiet'
            , 'file_args': [Util.conn_args_file] }
    }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('ybtools arguments')
        args_grp.add_argument("--ybtool_cmd", required=True, nargs=argparse.REMAINDER
            , help='ybtool command to execute')

    def execute(self):
        self.db_conn.env['conn_db'] = self.db_conn.database
        self.db_conn.env['dbuser'] = self.db_conn.ybdb['user']

        cmd_results = self.db_conn.ybtool_cmd(' '.join(self.args_handler.args.ybtool_cmd), stdin=True)
        cmd_results.on_error_exit()

        sys.stdout.write(cmd_results.stdout)
        sys.stderr.write(cmd_results.stderr)

def main():
    eybt = ExecYBTool().execute()
    exit(0)

if __name__ == "__main__":
    main()
