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
from yb_get_table_names import get_table_names
from yb_get_view_names import get_view_names
from yb_get_sequence_names import get_sequence_names


class ddl_object:
    """Issue the command used to dump out the SQL/DDL that was used to create a
    given object.
    """

    def __init__(self, object_type, common=None, db_args=None):
        """Initialize ddl_object class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        self.object_type = object_type
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()
            self.args_process()

    def args_process(self):
        self.common.args_process_init(
            description=('Return the {obj_type}/s DDL for the requested '
                         'database.  Use {obj_type} filters to limit the set '
                         'of tables returned.').format(obj_type=self.object_type))

        self.common.args_add_positional_args()
        self.common.args_add_optional()
        self.common.args_add_connection_group()
        
        args_ddl_grp = self.common.args_parser.add_argument_group('DDL arguments')
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

        self.db_args = yb_common.db_args(
            required_args_single=[]
            , optional_args_single=[]
            , optional_args_multi=[self.object_type, 'schema']
            , common=self.common)

        self.common.args_process()

        if self.common.args.schema_name:
            self.common.args.with_schema = True
        if self.common.args.db_name:
            self.common.args.with_db = True

    def exec(self):
        describe_sql = self.get_describe_sql(self.common)
        self.cmd_results = self.common.ybsql_query(describe_sql)

        if self.cmd_results.stdout != '':
            self.cmd_results.stdout = self.ddl_modifications(
                self.cmd_results.stdout, self.common)

    def get_describe_sql(self, common):
        """Build up SQL DESCRIBE statement/s.

        :param common: The instance of the `common` class constructed in this
                       module
        :return: A string containing the SQL DESCRIBE statement
        """
        #if argv is double quoted it needs to also have outer single quotes
        code = ('get_{object_type}_names'
            '(common=self.common, db_args=self.db_args)').format(
            object_type=self.object_type)
        gons = eval(code)
        gons.exec()

        if (gons.cmd_results.stderr != ''
            or gons.cmd_results.exit_code != 0):
            sys.stdout.write(text.color(gons.cmd_results.stderr, fg='red'))
            exit(gons.cmd_results.exit_code)

        objects = common.quote_object_path(gons.cmd_results.stdout)
        describe_objects = []
        if objects.strip() != '':
            for object in objects.strip().split('\n'):
                describe_objects.append('DESCRIBE %s ONLY DDL;\n\\echo' % object)

        return '\n'.join(describe_objects)

    def ddl_modifications(self, ddl, common):
        """
        Modify a given DDL statement by optionally adding db/schema name to a
        CREATE statement and transforming all SQL reserved words to uppercase.

        :param ddl: The DDL statement to modify
        :param common: The instance of the `common` class constructed in this
                       module
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
                if common.args.with_schema or common.args.with_db:
                    tablepath = (
                        ( common.args.schema_name
                          if common.args.schema_name
                          else ddl_schema)
                        + '.' + tablepath
                    )
                if common.args.with_db:
                    tablepath = (
                        ( common.args.db_name
                          if common.args.db_name
                          else common.database)
                        + '.' + tablepath
                    )
                tablepath = common.quote_object_path(tablepath)
                line = 'CREATE %s %s%s' % (matches.group(1), tablepath, matches.group(3))

            #change all data type key words to upper case 
            d_types = [
                'bigint', 'integer', 'smallint', 'numeric', 'real',
                'double precision', 'uuid', 'character varying', 'character',
                'date', 'time without time zone',
                'timestamp without time zone', 'timestamp with time zone',
                'ipv4', 'ipv6', 'macaddr', 'macaddr8'
            ]
            for data_type in d_types:
                line = re.sub(r"( )" + data_type + "(,?$|\()",
                              r"\1%s\2" % data_type.upper(), line)

            new_ddl.append(line)

        new_ddl = '\n'.join(new_ddl).strip() + '\n'

        #remove DDL comments at the beginning of each object definition
        new_ddl = re.sub(r"--.*?\n", "", new_ddl)
        #correct trailing ';' at end of each definition to be consistent
        new_ddl = re.sub(r"(\s*);", ";", new_ddl)

        return new_ddl


def main(object_type):
    ddlo = ddl_object(object_type)
    ddlo.exec()

    ddlo.cmd_results.write()

    exit(ddlo.cmd_results.exit_code)