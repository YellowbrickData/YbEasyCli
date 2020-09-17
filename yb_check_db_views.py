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
    """TODO doc
    """

    def __init__(self):

        db_ct = 0
        broken_view_ct = 0

        common = self.init_common()

        filter_clause = self.db_args.build_sql_filter(
            {'db':'db_name'},
            indent='    ')

        sql_query = (("""
SELECT
    name AS db_name
FROM
    sys.database
WHERE
    <filter>
ORDER BY
    name""").replace('<filter>', filter_clause))

        cmd_results = common.ybsql_query(sql_query)

        if cmd_results.exit_code != 0:
            sys.stdout.write(text.color(cmd_results.stderr, fg='red'))
            exit(cmd_results.exit_code)

        dbs = cmd_results.stdout.strip()
        if dbs == '' and self.db_args.has_optional_args_multi_set('db'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + common.database + '"']
        else:
            dbs = dbs.split('\n')

        self.db_args.schema_set_all_if_none()

        filter_clause = self.db_args.build_sql_filter(
            {'owner':'ownername','schema':'schemaname','view':'viewname'},
            indent='    ')

        db_ct = 0
        broken_view_ct = 0
        sys.stdout.write('-- Running broken view check.\n')
        for db in dbs:
            db_ct += 1
            cmd_results = common.call_stored_proc_as_anonymous_block(
                'yb_check_db_views_p'
                , args = {'a_filter':filter_clause}
                , pre_sql = ('\c %s\n' % db))

            if cmd_results.exit_code == 0:
                sys.stdout.write(common.quote_object_path(cmd_results.stdout))
            elif cmd_results.stderr.find('permission denied') == -1:
                sys.stderr.write(text.color(cmd_results.stderr, fg='red'))
                exit(cmd_results.exit_code)

            db_broken_view_ct = len(cmd_results.stdout.split())
            broken_view_ct += db_broken_view_ct
            sys.stdout.write('-- %d broken view/s in "%s".\n' % (db_broken_view_ct, db))
        sys.stdout.write('-- Completed check, found %d broken view/s in %d db/s.\n' % (broken_view_ct, db_ct))
    
        exit(cmd_results.exit_code)


    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        self.db_args = common.db_args(
            description='Check for broken views.'
            , positional_args_usage=[]
            , optional_args_multi=['owner', 'db', 'schema', 'view'])

        common.args_process()

        return common


check_db_views()
