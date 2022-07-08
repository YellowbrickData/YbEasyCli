#!/usr/bin/env python3
"""
USAGE:
      yb_check_db_views.py [options]

PURPOSE:
      Check for broken views.

OPTIONS:
      See the command line help message for all options.
      (yb_check_db_views.py --help)

Output:
      Various column statistics for desired table/s column/s.
"""
import sys

from yb_common import ArgsHandler, Common, StoredProc, Util
from yb_ddl_object import ddl_object

class check_db_views(Util):
    """Check for broken views.
    """
    config = {
        'description': 'Check for broken views.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --database_in stores'
            , 'file_args': [Util.conn_args_file] }
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
            cmd_results = StoredProc('yb_check_db_views_p', self.db_conn).call_proc_as_anonymous_block(
                args = {'a_filter':self.db_filter_sql()}
                , pre_sql = ('\c %s\n' % db))

            broken_views = []
            if cmd_results.exit_code == 0:
                if len(cmd_results.stdout.strip()):
                    for view_text in cmd_results.stdout.strip().split('\n'):
                        view = view_text.split('|')
                        broken_views.append({
                            "path":       Common.quote_object_paths(view[0].strip('- '))
                            , "sqlstate": view[1]
                            , "sqlerrm":  '|'.join(view[2:])})
                        sys.stdout.write('-- view: %s, sqlstate: %s, sqlerrm: %s\n' 
                            % (broken_views[-1]["path"], broken_views[-1]["sqlstate"], broken_views[-1]["sqlerrm"]))
            elif cmd_results.stderr.find('permission denied') == -1:
                Common.error(cmd_results.stderr)
                exit(cmd_results.exit_code)

            db_broken_view_ct = len(broken_views)
            broken_view_ct += db_broken_view_ct
            sys.stdout.write('-- %d broken view/s in database "%s".\n' % (db_broken_view_ct, db))

            if (self.args_handler.args.fix_views):
                for view in broken_views:
                    self.replace_broken_view(view)

        sys.stdout.write('-- Completed check, found %d broken view/s in %d db/s.\n' % (broken_view_ct, db_ct))

    def additional_args(self):
        args_ddl_grp = self.args_handler.args_parser.add_argument_group('optional arguments')
        args_ddl_grp.add_argument("--fix_views"
            , action='store_true', help="attempts to fix broken views by running a 'CREATE OR REPLACE' with the view ddl"
            ", defaults to False")

    def replace_broken_view(self, view):
        view_path = view['path'].split('.')

        orig_conn_db = self.db_conn.env['conn_db']
        orig_database = self.db_conn.database
        self.db_conn.env['conn_db'] = view_path[0].strip('" ')
        self.db_conn.database = self.db_conn.env['conn_db']

        ddl = self.get_ddl_view(view_path)
        cmd_result = self.db_conn.ybsql_query(ddl)
        print('-- fixing view: %s' % view['path'])
        print(ddl)
        print(cmd_result.stderr)

        self.db_conn.env['conn_db'] = orig_conn_db
        self.db_conn.database = orig_database        

    def get_ddl_view(self, view_path):
        args_handler = ArgsHandler(check_db_views.config)
        args_handler.args = lambda:None
        args_handler.args.template = '{ddl}'
        args_handler.args.exec_output = False
        args_handler.args.or_replace = True
        args_handler.args.with_db = True
        args_handler.args.with_schema = False
        args_handler.args.schema_name = None
        args_handler.args.db_name = None
        args_handler.args.schema_in_list = [[view_path[1].strip('" ')]]
        args_handler.args.schema_like_pattern = None
        args_handler.args.schema_not_in_list = []
        args_handler.args.schema_not_like_pattern = None
        args_handler.args.owner_in_list = []
        args_handler.args.owner_like_pattern = None
        args_handler.args.owner_not_in_list = []
        args_handler.args.owner_not_like_pattern = None
        args_handler.args.view_in_list = [[view_path[2].strip('" ')]]
        args_handler.args.view_like_pattern = None
        args_handler.args.view_not_in_list = []
        args_handler.args.view_not_like_pattern = None

        util_name='ddl_view'
        ddlo = ddl_object(util_name=util_name, init_default=False, db_conn=self.db_conn, args_handler=args_handler)
        ddlo.init(object_type=util_name[4:], db_conn=self.db_conn, args_handler=args_handler)

        return(ddlo.execute())

def main():
    cdbv = check_db_views()
    cdbv.execute()
    exit(0)


if __name__ == "__main__":
    main()
