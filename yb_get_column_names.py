#!/usr/bin/env python3
"""
USAGE:
      yb_get_column_names.py [database] object [options]

PURPOSE:
      List the column names comprising an object.

OPTIONS:
      See the command line help message for all options.
      (yb_get_column_names.py --help)

Output:
      The column names for the object will be listed out, one per line.
"""

import sys

import yb_common


class get_column_names:
    """Issue the ybsql command used to list the column names comprising an
    object.
    """
    template_default = '<column>'

    def __init__(self, db_conn=None, args_handler=None):
        """Initialize get_column_names class.

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
                    'List/Verifies that the specified column names exist.',
                optional_args_multi=['owner', 'column'],
                positional_args_usage='[database] object')

            self.add_args()

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
        self.db_filter_args = self.args_handler.db_filter_args

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter({
            'owner':'tableowner'
            ,'schema':'schemaname'
            ,'object':'objectname'
            ,'column':'columnname'}
            , indent='    ')

        sql_query = """
WITH
objct AS (
    SELECT
        a.attname AS columnname
        , a.attnum AS columnnum
        , c.relname AS objectname
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN {database_name}.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN ('r', 'v')
        AND a.attnum > 0
)
SELECT
    '{database_name}.' || schemaname || '.' || objectname || '.' || columnname AS column_path
FROM
    objct
WHERE
    objectname = '{object_name}'
    AND {filter_clause}
ORDER BY
    LOWER(schemaname), LOWER(objectname), columnnum""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database
             , object_name = self.args_handler.args.object)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)

        if self.cmd_results.stderr == '' and self.cmd_results.exit_code == 0:
            self.cmd_results.stdout = yb_common.common.apply_template(
                self.cmd_results.stdout
                , self.args_handler.args.template
                , ['table_path', 'schema_path', 'column', 'table', 'schema', 'database'])
            if self.args_handler.args.template == self.template_default:
                self.cmd_results.stdout = yb_common.common.quote_object_paths(self.cmd_results.stdout)
            if self.args_handler.args.exec_output:
                self.cmd_results = self.db_conn.ybsql_query(self.cmd_results.stdout)

    def add_args(self):
        args_optional_grp = self.args_handler.args_parser.add_argument_group(
            'optional arguments')
        # move group in help display from the last position up 1
        group_index = len(self.args_handler.args_parser._action_groups) - 1
        self.args_handler.args_parser._action_groups.insert(
            group_index - 1, self.args_handler.args_parser._action_groups.pop(group_index))

        args_optional_grp.add_argument(
            "--output_template", metavar='template', dest='template'
            , help="template used to print output"
                ", defaults to '%s'"
                ", template variables include; <table_path>"
                ", <schema_path>, <column>, <table>, <schema>, and <database>" % self.template_default
            , default=self.template_default)
        args_optional_grp.add_argument(
            "--exec_output", action="store_true"
            , help="execute output as SQL, defaults to FALSE")

def main():
    gcns = get_column_names()
    gcns.execute()

    gcns.cmd_results.write()

    exit(gcns.cmd_results.exit_code)


if __name__ == "__main__":
    main()