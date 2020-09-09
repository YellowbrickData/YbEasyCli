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


class ddl_object:
    """Issue the command used to dump out the SQL/DDL that was used to create a
    given object.
    """

    def __init__(self, object_type):
        self.object_type = object_type
        common = self.init_common()

        describe_sql = self.get_describe_sql(common)
        cmd_results = common.ybsql_query(describe_sql)

        if cmd_results.stdout != '':
            ddl_sql = self.ddl_modifications(cmd_results.stdout, common)
            sys.stdout.write(ddl_sql)
        if cmd_results.stderr != '':
            sys.stderr.write(common.color(cmd_results.stderr, fg='red'))

        exit(cmd_results.exit_code)

    def get_describe_sql(self, common):
        """Build up a SQL DESCRIBE statement.

        :param common: The instance of the `common` class constructed in this
                       module
        :return: A string containing the SQL DESCRIBE statement
        """
        #if argv is double quoted it needs to also have outer single quotes
        util_cmd = ''
        skip_next_argv = False
        for argv in sys.argv:
            if skip_next_argv:
                skip_next_argv = False
            elif argv in ['--with_db', '--with_schema']:
                None
            elif argv in ['--db_name', '--schema_name']:
                skip_next_argv = True
            elif argv[0:1] == '"':
                util_cmd += "'" + argv + "'" + ' '
            else:
                util_cmd += argv + ' '

        util_cmd = (util_cmd
                    .replace(os.path.basename(sys.argv[0]),
                             'yb_get_%s_names.py' % self.object_type))
        cmd_results = common.call_util_cmd(util_cmd)

        describe_objects = []
        if cmd_results.stdout.strip() != '':
            for object in cmd_results.stdout.strip().split('\n'):
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

    def init_common(self):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        common.args_process_init(
            description=('Return the {obj_type}/s DDL for the requested '
                         'database.  Use {obj_type} filters to limit the set '
                         'of tables returned.').format(obj_type=self.object_type))

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        
        args_ddl_grp = common.args_parser.add_argument_group('DDL arguments')
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
            required_args_single=[],
            optional_args_single=[],
            optional_args_multi=[self.object_type, 'schema'],
            common=common)

        common.args_process()

        if common.args.schema_name:
            common.args.with_schema = True
        if common.args.db_name:
            common.args.with_db = True

        return common
