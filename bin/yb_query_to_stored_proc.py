#!/usr/bin/env python3
"""
USAGE:
      yb_query_to_stored_proc.py [options]

PURPOSE:
      Create a stored procedure that runs the provided query with the permissions of the
      definer/creator.

OPTIONS:
      See the command line help message for all options.
      (yb_query_to_table.py --help)

Output:
      A stored procedure.
"""
from yb_common import ArgIntRange, Common, Text, StoredProc, Util

class query_to_stored_proc(Util):
    """Issue the ybsql command used to create a stored procedure that runs the provided 
    query with the permissions of the definer/creator.
    """
    config = {
        'description': (
            'Create a stored procedure for the provided query with the query privileges of the definer/creator.'
            '\n'
            '\nnote:'
            '\n  when creating the stored procedure an empty table named <stored procedure>_t will also be created'
            '\n  to facilitate the SETOF return type of the stored procedure.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --stored_proc log_session_p @$HOME/session_query.arg"
            , 'file_args': [Util.conn_args_file
                , { '$HOME/session_query.arg': """--query \"\"\"
SELECT
    s.session_id
    , u.name AS user, d.name AS database, s.application_name
    , s.start_time, s.end_time - s.start_time AS session_duration
FROM
    sys.log_session AS s
    LEFT JOIN sys.user AS u
        USING (user_id)
    LEFT JOIN sys.database AS d
        USING (database_id)
ORDER BY session_duration DESC
\"\"\""""} ] } }
 
    def execute_drop(self):
        print('-- Dropping the %s stored procedure.'
            % Text.color(self.stored_proc, style='bold'))
        self.cmd_results = self.db_conn.ybsql_query('DROP TABLE %s_t CASCADE' % self.stored_proc)
        self.cmd_results.write(tail='-- Dropped.\n')

    def execute_create(self):
        print('-- Creating the %s stored procedure for the query provided.'
            % Text.color(self.stored_proc, style='bold'))

        stored_proc_template_file = ('%s/%s'
            % (Common.util_dir_path, '../sql/yb_query_to_stored_proc_template_1_p.sql'))
        stored_proc_template = Common.read_file(stored_proc_template_file)

        self.cmd_results = StoredProc('yb_query_to_stored_proc_p', self.db_conn).call_proc_as_anonymous_block(
            args = {
                'a_query' : self.args_handler.args.query
                , 'a_stored_proc_name' : self.stored_proc
                , 'a_stored_proc_template' : stored_proc_template
                , 'a_max_varchar_size' : self.args_handler.args.max_varchar_size
                , 'a_limit_default' : self.args_handler.args.query_limit_default
                , 'a_grant_execute_to' : self.args_handler.args.grant_execute_to}
            , pre_sql = self.args_handler.args.pre_sql
            , post_sql = self.args_handler.args.post_sql)

        self.cmd_results.write(tail='-- Created.\n')

    def additional_args(self):
        args_required_grp = self.args_handler.args_parser.add_argument_group(
            'required argument')
        args_required_grp.add_argument(
            "--stored_proc", required=True
            , help="stored procedure name, the name of the stored procedure that will be created/deleted")

        args_required_create_grp = self.args_handler.args_parser.add_argument_group(
            'required arguments for creating a stored procedure')
        args_required_create_grp.add_argument(
            "--query"
            , help="query to build into a stored procedure")

        args_optional_grp = self.args_handler.args_parser.add_argument_group(
            'optional arguments for creating a stored procedure')
        args_optional_grp.add_argument("--grant_execute_to"
            , nargs="+", default='public', metavar='ROLE'
            , help="grant execute of stored procedure to user/roles, defaults to 'public'")
        args_optional_grp.add_argument("--query_limit_default"
            , type=ArgIntRange(0,9223372036854775807), default=200
            , help="default row limit for query, defaults to 200, set to 0 for unlimited")
        args_optional_grp.add_argument("--max_varchar_size"
            , type=ArgIntRange(1,64000), default=10000
            , help="truncate size of all VARCHAR columns in the destination table, defaults to 10000")
        args_optional_grp.add_argument("--pre_sql", default=''
            , help="SQL to run before the creation of the stored procedure")
        args_optional_grp.add_argument("--post_sql", default=''
            , help="SQL to run after the creation of the stored procedure")

        args_required_drop_grp = self.args_handler.args_parser.add_argument_group(
            'required arguments for dropping a stored procedure')
        args_required_drop_grp.add_argument("--drop", action="store_true"
            , help="drop stored procedure and supporting SETOF table")

    def additional_args_process(self):
        if (not(bool(self.args_handler.args.query) or (self.args_handler.args.drop))
            or (bool(self.args_handler.args.query) and (self.args_handler.args.drop))):
            self.args_handler.args_parser.error('one of --query or the --drop options must be provided...')

        self.stored_proc = self.args_handler.args.stored_proc

        # refomrat roles, double quote names with upper case, change list to comma delimited string 
        if isinstance(self.args_handler.args.grant_execute_to, list): 
            self.args_handler.args.grant_execute_to = '\n'.join(self.args_handler.args.grant_execute_to)
        self.args_handler.args.grant_execute_to = (
            Common.quote_object_paths(self.args_handler.args.grant_execute_to).replace('\n', ', '))

def main():
    dsp = query_to_stored_proc()

    if dsp.args_handler.args.drop:
        dsp.execute_drop()
    elif dsp.args_handler.args.query:
        dsp.execute_create()
        exit(dsp.cmd_results.exit_code)


if __name__ == "__main__":
    main()