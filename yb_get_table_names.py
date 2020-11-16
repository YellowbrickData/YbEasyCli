#!/usr/bin/env python3
"""
USAGE:
      yb_get_table_names.py [database] [options]

PURPOSE:
      List the table names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      The fully qualified names of all tables will be listed out, one per line.
"""

import sys

import yb_common


class get_table_names:
    """Issue the command used to list the table names found in a particular
    database.
    """
    template_default = '<table_path>'

    def __init__(self, db_conn=None, args_handler=None):
        """Initialize get_table_names class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        exec
        """
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handler
            if not hasattr(self.args_handler.args, 'template'):
                self.args_handler.args.template = self.template_default
            if not hasattr(self.args_handler.args, 'exec_output'):
                self.args_handler.args.exec_output = False
        else:            
            self.args_handler = yb_common.args_handler(
                description=
                    'List/Verifies that the specified table/s exist.'
                , optional_args_multi=['owner', 'schema', 'table'])

            self.add_args()
            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
        self.db_filter_args = self.args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'}
            , indent='    ')

        sql_query = """
SELECT
    '{database_name}.' || c.schemaname || '.' || c.tablename AS table_path
FROM
    {database_name}.information_schema.tables AS t
    JOIN {database_name}.pg_catalog.pg_tables AS c
        ON (t.table_name = c.tablename AND t.table_schema = c.schemaname)
WHERE
    t.table_type='BASE TABLE'
    AND {filter_clause}
ORDER BY LOWER(c.schemaname), LOWER(c.tablename)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

        if self.cmd_results.stderr == '' and self.cmd_results.exit_code == 0:
            self.cmd_results.stdout = yb_common.common.apply_template(
                self.cmd_results.stdout
                , self.args_handler.args.template
                , ['table_path', 'schema_path', 'table', 'schema', 'database'])
            if self.args_handler.args.exec_output:
                self.cmd_results = self.db_conn.ybsql_query(self.cmd_results.stdout)

    def add_args(self):
        args_optional_grp = self.args_handler.args_parser.add_argument_group(
            'optional arguments')
        group_index = len(self.args_handler.args_parser._action_groups) - 1
        self.args_handler.args_parser._action_groups.insert(
            group_index - 1, self.args_handler.args_parser._action_groups.pop(group_index))

        args_optional_grp.add_argument(
            "--output_template", metavar='template', dest='template'
            , help="template used to print output"
                ", defaults to '%s'"
                ", template variables include; <table_path>"
                ", <schema_path>, <table>, <schema>, and <database>" % self.template_default
            , default=self.template_default)
        args_optional_grp.add_argument(
            "--exec_output", action="store_true"
            , help="execute output as SQL, defaults to FALSE")

def main():
    gtns = get_table_names()
    gtns.execute()

    gtns.cmd_results.write()

    exit(gtns.cmd_results.exit_code)


if __name__ == "__main__":
    main()