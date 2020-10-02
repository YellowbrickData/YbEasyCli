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
import os
import re

import yb_common
from yb_common import text

class check_db_views:
    """Check for broken views.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize check_db_views class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()

            self.db_args = self.common.db_args(
                description='Check for broken views.'
                , positional_args_usage=[]
                , optional_args_multi=['owner', 'database', 'schema', 'view'])

            self.common.args_process()

        self.db_conn = yb_common.db_connect(self.common.args)

    def get_dbs(self):
        db_ct = 0
        broken_view_ct = 0

        filter_clause = self.db_args.build_sql_filter(
            {'database':'db_name'}
            , indent='    ')

        sql_query = """
SELECT
    name AS db_name
FROM
    sys.database
WHERE
    {filter_clause}
ORDER BY
    name""".format(filter_clause = filter_clause)

        cmd_results = self.db_conn.ybsql_query(sql_query)

        if cmd_results.exit_code != 0:
            sys.stdout.write(text.color(cmd_results.stderr, fg='red'))
            exit(cmd_results.exit_code)

        dbs = cmd_results.stdout.strip()
        if dbs == '' and self.db_args.has_optional_args_multi_set('database'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + self.db_conn.database + '"']
        else:
            dbs = dbs.split('\n')

        return dbs

    def execute(self):
        dbs = self.get_dbs()

        self.db_args.schema_set_all_if_none()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'ownername','schema':'schemaname','view':'viewname'}
            , indent='    ')

        db_ct = 0
        broken_view_ct = 0
        sys.stdout.write('-- Running broken view check.\n')
        for db in dbs:
            db_ct += 1
            cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_check_db_views_p'
                , args = {'a_filter':filter_clause}
                , pre_sql = ('\c %s\n' % db))

            if cmd_results.exit_code == 0:
                sys.stdout.write(self.common.quote_object_paths(cmd_results.stdout))
            elif cmd_results.stderr.find('permission denied') == -1:
                sys.stderr.write(text.color(cmd_results.stderr, fg='red'))
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
