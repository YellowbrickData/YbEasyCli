#!/usr/bin/env python3
"""
This module is used to dump out the SQL/DDL that was used to create any object
(of any type).

End users typically will not use this module directly. Instead, they will use
wrapper scripts that utilize this module. These include
    - yb_ddl_table.py
    - yb_ddl_view.py
    - yb_ddl_sequence.py
"""

import os
import re
import sys

import yb_common
from yb_common import text
from yb_util import util
from yb_get_table_names import get_table_names
from yb_get_view_names import get_view_names
from yb_get_sequence_names import get_sequence_names

class ddl_object(util):
    """Issue the command used to dump out the SQL/DDL that was used to create a
    given object.
    """

    def init(self, object_type, db_conn=None, args_handler=None):
        """Initialize ddl_object class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        self.object_type = object_type
        if db_conn:
            self.db_conn = db_conn
            self.args_handler = args_handle
        else:
            self.args_handler = yb_common.args_handler(self.config)
            self.args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)

    def args_add_by_object_type(self, args_grp):
        if self.object_type == 'table':
            args_grp.add_argument("--with_rowcount"
                , action="store_true"
                , help="display the current rowcount")

    def args_process(self):
        self.args_handler.args_process_init()
        self.args_handler.args_add_positional_args()
        self.args_handler.args_add_optional()
        self.args_handler.args_add_connection_group()
        
        args_ddl_grp = self.args_handler.args_parser.add_argument_group('optional DDL arguments')
        args_ddl_grp.add_argument("--with_schema",
                                  action='store_true',
                                  help="add the schema name to the %s DDL"
                                  % self.object_type)
        args_ddl_grp.add_argument("--with_db",
                                  action='store_true',
                                  help="add the database name to the %s DDL"
                                  % self.object_type)
        args_ddl_grp.add_argument("--schema_name",
                                  help="set a new schema name to the %s DDL"
                                  % self.object_type)
        args_ddl_grp.add_argument("--db_name",
                                  help="set a new database name to the %s DDL"
                                  % self.object_type)
        self.args_add_by_object_type(args_ddl_grp)

        self.args_handler.db_filter_args = yb_common.db_filter_args(
            required_args_single=[]
            , optional_args_single=[]
            , optional_args_multi=[self.object_type, 'schema']
            , args_handler=self.args_handler)

        self.args_handler.args_process()

        if self.args_handler.args.schema_name:
            self.args_handler.args.with_schema = True
        if self.args_handler.args.db_name:
            self.args_handler.args.with_db = True

    def execute(self):
        describe_sql = self.get_describe_sql()
        self.cmd_results = self.db_conn.ybsql_query(describe_sql)

        if self.cmd_results.stdout != '':
            self.cmd_results.stdout = self.ddl_modifications(
                self.cmd_results.stdout, self.args_handler.args)

    def get_describe_sql_by_object_type(self, object):
        describe_clause = 'DESCRIBE %s ONLY DDL;\n\\echo' % object

        if self.object_type == 'table':
            if self.args_handler.args.with_rowcount:
                rowcount_sql = ('SELECT COUNT(*) FROM %s' % object)
                cmd_results = self.db_conn.ybsql_query(rowcount_sql)
                describe_clause = """SELECT '--Rowcount: %s  Table: %s  At: ' || NOW() || '';\n%s""" % (
                    format(int(cmd_results.stdout), ",d"), object, describe_clause)

        return describe_clause

    def get_describe_sql(self):
        """Build up SQL DESCRIBE statement/s.

        :param common: The instance of the `common` class constructed in this
                       module
        :return: A string containing the SQL DESCRIBE statement
        """
        code = ('get_{object_type}_names'
            '(db_conn=self.db_conn, args_handler=self.args_handler)').format(
            object_type=self.object_type)
        gons = eval(code)
        gons.execute()

        if (gons.cmd_results.stderr != ''
            or gons.cmd_results.exit_code != 0):
            sys.stdout.write(text.color(gons.cmd_results.stderr, fg='red'))
            exit(gons.cmd_results.exit_code)

        objects = yb_common.common.quote_object_paths(gons.cmd_results.stdout)
        describe_objects = []
        if objects.strip() != '':
            for object in objects.strip().split('\n'):
                describe_clause = self.get_describe_sql_by_object_type(object)
                describe_objects.append(describe_clause)

        return '\n'.join(describe_objects)

    def ddl_modifications(self, ddl, args):
        """
        Modify a given DDL statement by optionally adding db/schema name to a
        CREATE statement and transforming all SQL reserved words to uppercase.

        :param ddl: The DDL statement to modify
        :param args: The command line args after being processed
        :return: A string containing the modified DDL statement
        """
        new_ddl = []
        ddl_schema = ''

        for line in ddl.split('\n'):
            token = line.split(':')
            if token[0] == '-- Schema':
                ddl_schema = token[1].strip()

            #add schema and database to object name and quote name where needed
            matches = re.match(r"\s*CREATE\s*([^\s]*)\s*([^\s(]*)(.*)"
                , line, re.MULTILINE)
            if matches:
                tablepath = matches.group(2)
                if args.with_schema or args.with_db:
                    tablepath = (
                        ( args.schema_name
                          if args.schema_name
                          else ddl_schema)
                        + '.' + tablepath
                    )
                if args.with_db:
                    tablepath = (
                        ( args.db_name
                          if args.db_name
                          else self.db_conn.database)
                        + '.' + tablepath
                    )
                tablepath = yb_common.common.quote_object_paths(tablepath)
                line = 'CREATE %s %s%s' % (matches.group(1), tablepath, matches.group(3))

            #change all data type key words to upper case 
            d_types = [
                'bigint', 'integer', 'smallint', 'numeric', 'real'
                , 'double precision', 'uuid', 'character varying', 'character'
                , 'date', 'time without time zone'
                , 'timestamp without time zone', 'timestamp with time zone'
                , 'ipv4', 'ipv6', 'macaddr', 'macaddr8'
                , 'boolean'
            ]
            for data_type in d_types:
                line = re.sub(r"( )" + data_type + "(,?$|\()",
                              r"\1%s\2" % data_type.upper(), line)

            new_ddl.append(line)

        new_ddl = '\n'.join(new_ddl).strip() + '\n'

        #remove DDL comments at the beginning of each object definition
        new_ddl = re.sub(r"--( |-).*?\n", "", new_ddl)
        #correct trailing ';' at end of each definition to be consistent
        new_ddl = re.sub(r"(\s*);", ";", new_ddl)

        return new_ddl


def main(util_name):
    ddlo = ddl_object(util_name=util_name, init_default=False)
    ddlo.init(object_type=util_name[4:])
    ddlo.execute()

    ddlo.cmd_results.write()

    exit(ddlo.cmd_results.exit_code)