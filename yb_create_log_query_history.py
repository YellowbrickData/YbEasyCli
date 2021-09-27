#!/usr/bin/env python3
"""
TODO 
USAGE:
      yb_create_log_query_history.py [options]

PURPOSE:
      Build/update long term history db table/views sourced from the sys.log_query view.

OPTIONS:
      See the command line help message for all options.
      (yb_create_log_query_history.py --help)

Output:
      Action taken, like:
          --created log_query_history table, log_query_history_text table and log_query_history_v view
          --inserted X queries into log_query_history and log_query_history_text
"""
import getpass, re

from yb_common import Common, DBConnect, Text, Util

class create_log_query_history(Util):
    """Build/update long term history db table/views sourced from the sys.log_query view.
    """
    config = {
        'description': (
            'Build/update long term history db table/views sourced from the sys.log_query view.'
            '\n'
            '\nnote:'
            '\n  On the first execution the create_log_query_history will;'
            '\n      1. request super user credentials to create supporting stored procs.'
            '\n      2. create the history query table, query_text table and query view.'
            '\n  Every run inserts new log queries into the history query and query_text tables.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': """@$HOME/conn.args --log_table_name user_log_query_hist --where_clause "username NOT LIKE 'sys_ybd_%'" """
            , 'file_args': [Util.conn_args_file] } }

    def additional_args(self):
        log_query_hist_grp = self.args_handler.args_parser.add_argument_group(
            'log query history arguments')
        log_query_hist_grp.add_argument("--log_table_name", default="log_query_history"
            , help="the object name prefix used for the 2 log tables and view, defaults to 'log_query_history'")
        log_query_hist_grp.add_argument("--where_clause", default="TRUE"
            , help=("where clause applied to sys.log_query to limit the queries for which history is maintained,"
                " defaults to 'TRUE' meaning all queries") )

    def complete_db_conn(self):
        if self.db_conn.ybdb['is_super_user']:
            self.args_handler.args_parser.error("dbuser '%s' must not ba a db super user..." % self.db_conn.ybdb['user'])
        return

    def create_log_query_history(self):
        result = self.db_conn.ybsql_query("""
SELECT create_log_query_history_p(
    '{log_table_name}'
    , $${where_clause}$$);""".format(
            log_table_name=Common.quote_object_paths(self.args_handler.args.log_table_name)
            , where_clause=self.args_handler.args.where_clause) )
        return(result)

    def create_su_db_conn(self):
        su_env = self.db_conn.env.copy()

        su_env['conn_db'] = self.db_conn.database

        su_env['dbuser'] = input("Enter the super user name to create required stored procs with: ") 
        prompt = ("Enter the password for cluster %s, user %s: "
            % (Text.color(su_env['host'], fg='cyan')
                , Text.color(su_env['dbuser'], fg='cyan')))
        su_env['pwd'] = getpass.getpass(prompt)

        DBConnect.set_env(su_env)
        self.su_db_conn = DBConnect(env=su_env, conn_type='su')
        DBConnect.set_env(self.db_conn.env_pre)

        if not self.su_db_conn.ybdb['is_super_user']:
            Common.error("dbuser '%s' is not a super user..." % su_env['dbuser'])

    def create_stored_procs(self):
        filename = '%s/sql/log_query_history/materialize_sys_log_query_p.sql' % Common.util_dir_path
        sql = open(filename).read()
        sql = ("""SET SCHEMA '%s';
            %s;
            GRANT EXECUTE ON PROCEDURE materialize_sys_log_query_p(VARCHAR, VARCHAR, VARCHAR, BOOLEAN) TO %s;"""
                % (self.db_conn.schema, sql, self.db_conn.env['dbuser']) )
        result = self.su_db_conn.ybsql_query(sql)
        result.on_error_exit()

        filename = '%s/sql/log_query_history/create_log_query_history_p.sql' % Common.util_dir_path
        sql = open(filename).read()
        result = self.db_conn.ybsql_query(sql)
        result.on_error_exit()

    def fix_stored_proc_stdout(self, result):
        """stored procs print everything to stderr.  This routine moves all stderr
        lines starting with 'INFO: --' to stdout."""
        matches = re.finditer(r"^(INFO:\s*)?(--.*)$", result.stderr, re.MULTILINE)

        stdout = ''
        stderr = ''
        for matchNum, match in enumerate(matches, start=1):
            if (match.group(1)):
                stdout = ('%s\n%s' % (stdout, match.group(2))) if len(stdout) else match.group(2)
            else:
                stderr = ('%s\n%s' % (stderr, match.group(2))) if len(stderr) else match.group(2)

        result.proc_return = result.stdout
        result.stdout = stdout if len(stdout.strip()) else ''
        result.stderr = stderr if len(stderr.strip()) else ''

    def execute(self):
        self.complete_db_conn()

        result = self.create_log_query_history()
        if re.search(r"create_log_query_history_p.*does not exist", result.stderr):
            self.create_su_db_conn()
            self.create_stored_procs()
            result = self.create_log_query_history()
        self.fix_stored_proc_stdout(result)

        result.on_error_exit()
        result.write()

        exit(result.exit_code)

def main():
    clqh = create_log_query_history()

    clqh.execute()


if __name__ == "__main__":
    main()