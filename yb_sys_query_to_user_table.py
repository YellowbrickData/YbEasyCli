#!/usr/bin/env python3
"""
USAGE:
      yb_sys_query_to_user_table.py [options]

PURPOSE:
      Materialize a system table or in memory query to a user table.

OPTIONS:
      See the command line help message for all options.
      (yb_sys_query_to_user_table.py --help)

Output:
      A column store table.
"""
import sys

from yb_common import Util, IntRange

class sys_query_to_user_table(Util):
    """Issue the ybsql command used to materialize a system table or in
    memory query to a user table.
    """
    config = {
        'description': 'Convert system query to user table.'
        , 'default_args': {'create_table': None, 'as_temp_table': None, 'drop_table': None
            , 'max_varchar_size': 10000, 'pre_sql': '', 'post_sql': ''}
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/sys_schema.args --'
            , 'file_args': [Util.conn_args_file
                , { '$HOME/sys_schema.args': """--query \"\"\"
SELECT name
FROM sys.schema
\"\"\"
--table 'sys_schema'"""} ] } }

    def execute(self):
        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
            'yb_sys_query_to_user_table_p'
            , args = {
                'a_query' : self.args_handler.args.query
                , 'a_tablename' : self.args_handler.args.table
                , 'a_create_table' : ('TRUE' if self.args_handler.args.create_table else 'FALSE')
                , 'a_as_temp_table' : ('TRUE' if self.args_handler.args.as_temp_table else 'FALSE')
                , 'a_drop_table' : ('TRUE' if self.args_handler.args.drop_table else 'FALSE')
                , 'a_max_varchar_size' : self.args_handler.args.max_varchar_size}
            , pre_sql = self.args_handler.args.pre_sql
            , post_sql = self.args_handler.args.post_sql)

    def additional_args(self):
        args_required_grp = self.args_handler.args_parser.add_argument_group(
            'required arguments')
        args_required_grp.add_argument(
            "--query", required=True
            , help="system query to convert to user table")
        args_required_grp.add_argument(
            "--table", required=True
            , help="table name, the name of the table that the data is inserted into")

        args_optional_grp = self.args_handler.args_parser.add_argument_group(
            'optional arguments')
        args_optional_grp.add_argument("--create_table", action="store_true"
            , help="create destination table, defaults to FALSE")
        args_optional_grp.add_argument("--as_temp_table", action="store_true"
            , help="create destination table as temporary table, defaults to FALSE")
        args_optional_grp.add_argument("--drop_table", action="store_true"
            , help="first drop the destination table if it exists, defaults to FALSE")
        args_optional_grp.add_argument("--max_varchar_size"
            , type=IntRange(1,64000), default=10000
            , help="truncate size of all VARCHAR columns in the destination table, defaults to 10000")
        args_optional_grp.add_argument("--pre_sql", default=''
            , help="SQL to run before the creation of the destination table")
        args_optional_grp.add_argument("--post_sql", default=''
            , help="SQL to run after the creation of the destination table")

def main():
    sqtout = sys_query_to_user_table()

    sys.stdout.write('-- Converting system query to user table.\n')
    sqtout.execute()
    sqtout.cmd_results.write(tail='-- The %s user table has been created.\n'
        % sqtout.args_handler.args.table)

    exit(sqtout.cmd_results.exit_code)


if __name__ == "__main__":
    main()