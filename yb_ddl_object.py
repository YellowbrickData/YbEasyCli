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

        common = self.init_common(object_type)

        describe_sql = self.get_describe_sql(common)
        cmd_results = common.ybsql_query(describe_sql)

        if cmd_results.exit_code == 0:
            ddl_sql = self.ddl_modifications(cmd_results.stdout, common)
            sys.stdout.write(ddl_sql)
        else:
            sys.stdout.write(common.color(cmd_results.stderr, fg='red'))
        exit(cmd_results.exit_code)

    def get_describe_sql(self, common):
        """Build up a SQL DESCRIBE statement.

        :param common: The instance of the `common` class constructed in this
                       module
        :return: A string containing the SQL DESCRIBE statement
        """
        util_cmd = (' '.join(sys.argv)
                    .replace(' --with_db', '')
                    .replace(' --with_schema', '')
                    .replace(os.path.basename(sys.argv[0]),
                             'yb_get_%s_names.py' % common.object_type))
        cmd_results = common.call_util_cmd(util_cmd)

        describe_objects = []
        for object in cmd_results.stdout.strip().split('\n'):
            if common.args.schemas:
                object_full_name = '%s.%s' % (common.database, object)
            else:
                object_full_name = '%s.%s.%s' % (common.database,
                                                 common.schema,
                                                 object)
            describe_objects.append('DESCRIBE %s ONLY DDL;\n\\echo' % object_full_name)

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
            create_object_clause = 'CREATE %s ' % common.object_type.upper()
            if common.args.with_schema or common.args.with_db:
                line = line.replace(create_object_clause,
                                    '%s%s.' % (create_object_clause,
                                               ddl_schema))
            if common.args.with_db:
                line = line.replace(create_object_clause,
                                    '%s%s.' % (create_object_clause,
                                               common.database))

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

    def init_common(self, object_type):
        """Initialize common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :param object_type: The type of the database object
        :return: An instance of the `common` class
        """
        common = yb_common.common(
            description=('Return the {obj_type}/s DDL for the requested '
                         'database.  Use {obj_type} filters to limit the set '
                         'of tables returned.').format(obj_type=object_type),
            object_type=object_type)

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()
        common.args_add_filter_group()

        args_ddl_grp = common.args_parser.add_argument_group('DDL arguments')
        args_ddl_grp.add_argument("--with_schema",
                                  action='store_true',
                                  help="add the schema name to the object DDL")
        args_ddl_grp.add_argument("--with_db",
                                  action='store_true',
                                  help="add the database name to the object DDL")

        common.args_process()

        return common
