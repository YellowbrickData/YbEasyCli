#!/usr/bin/env python3
"""
USAGE:
      yb_find_columns.py [database] [options]

PURPOSE:
      List all columns found for the provided filter.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The column names and column attributes for filtered columns.
"""

import sys

import yb_common


class find_columns:
    """Issue the ybsql command used to list the column names comprising an
    object.
    """
    template_default = '-- Table: <table_path>, Column: <column>, Table Ordinal: <ordinal>, Data Type: <data_type>'

    def __init__(self, db_conn=None, args_handler=None):
        """Initialize find_columns class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
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
                    'List column names and column attributes for filtered columns.',
                optional_args_multi=['owner', 'schema', 'table', 'column', 'datatype'],
                positional_args_usage='[database]')

            self.add_args()

            self.args_handler.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
        self.db_filter_args = self.args_handler.db_filter_args

        self.db_filter_args.schema_set_all_if_none()

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(
            {
                'owner':'tableowner'
                ,'schema':'schemaname'
                ,'table':'tablename'
                ,'column':'columnname'
                ,'datatype':'datatype'}
            , indent='    ')

        self.cmd_results = self.db_conn.call_stored_proc_as_anonymous_block(
                'yb_find_columns_p'
                , args = {
                    'a_column_filter_clause' : filter_clause
                }
            )

        if self.cmd_results.stderr == '' and self.cmd_results.exit_code == 0:
            self.cmd_results.stdout = yb_common.common.apply_template(
                self.cmd_results.stdout
                , self.args_handler.args.template
                , ['table_path', 'schema_path', 'column', 'ordinal', 'data_type', 'table', 'schema', 'database'])
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
                ", <schema_path>, <column>, <ordinal>, <data_type>, <table>, <schema>, and <database>" % self.template_default
            , default=self.template_default)
        args_optional_grp.add_argument(
            "--exec_output", action="store_true"
            , help="execute output as SQL, defaults to FALSE")


def main():
    fcs = find_columns()
    fcs.execute()

    fcs.cmd_results.write()

    exit(fcs.cmd_results.exit_code)


if __name__ == "__main__":
    main()