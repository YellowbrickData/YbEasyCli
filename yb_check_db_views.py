#!/usr/bin/env python3
"""
USAGE:
      yb_check_db_views.py [database] table [options]

PURPOSE:
      Check for broken views.

OPTIONS:
      See the command line help message for all options.
      (yb_check_db_views.py --help)

Output:
      Various column statistics for desired table/s column/s.
"""
import sys

import yb_common
from yb_util import util

class check_db_views(util):
    """Check for broken views.
    """
    config = {
        'description': 'Check for broken views.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --database_in stores'
            , 'file_args': [util.conn_args_file] }
        , 'db_filter_args': {'owner':'ownername','schema':'schemaname','view':'viewname'}
    }

    def execute(self):
        dbs = self.get_dbs()

        self.db_filter_args.schema_set_all_if_none()

        db_ct = 0
        broken_view_ct = 0
        sys.stdout.write('-- Running broken view check.\n')
        for db in dbs:
            db_ct += 1
            cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_check_db_views_p'
                , args = {'a_filter':self.db_filter_sql()}
                , pre_sql = ('\c %s\n' % db))

            if cmd_results.exit_code == 0:
                sys.stdout.write(yb_common.common.quote_object_paths(cmd_results.stdout))
            elif cmd_results.stderr.find('permission denied') == -1:
                yb_common.common.error(cmd_results.stderr)
                exit(cmd_results.exit_code)

            db_broken_view_ct = len(cmd_results.stdout.split())
            broken_view_ct += db_broken_view_ct
            sys.stdout.write('-- %d broken view/s in "%s".\n' % (db_broken_view_ct, db))
        sys.stdout.write('-- Completed check, found %d broken view/s in %d db/s.\n' % (broken_view_ct, db_ct))


def main():
    cdbv = check_db_views()
    cdbv.execute()
    exit(0)


if __name__ == "__main__":
    main()
