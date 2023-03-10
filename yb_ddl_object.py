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

import argparse
import os
import re
import sys
import copy
# fix for deepcopy in python 2.7
copy._deepcopy_dispatch[type(re.compile(''))] = lambda r, _: r

from yb_common import Common, Text, Util
from yb_get_table_names import get_table_names
from yb_get_view_names import get_view_names
from yb_get_sequence_names import get_sequence_names

class ddl_object(Util):
    """Issue the command used to dump out the SQL/DDL that was used to create a
    given object.
    """

    stored_proc_describe_query = """WITH
stored_proc_describe AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(n.nspname), LOWER(p.proname)) AS ordinal
        , n.nspname AS schema
        , p.proname AS stored_proc
        , pg_catalog.pg_get_userbyid(p.proowner) AS owner
        , pg_catalog.pg_get_functiondef(p.oid) AS raw_ddl
        , CASE
            WHEN p.proisagg THEN 'agg'
            WHEN p.proiswindow THEN 'window'
            WHEN p.prosp THEN 'stored procedure'
            WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
                ELSE 'normal'
        END AS type
        , '-- Schema: ' || schema
        || CHR(10) || 'CREATE PROCEDURE '
        || stored_proc || REPLACE(REGEXP_REPLACE(raw_ddl, '[^(]*', ''), '$function$', '$CODE$') AS ddl
    FROM
        {database}.pg_catalog.pg_proc AS p
        LEFT JOIN {database}.pg_catalog.pg_namespace AS n
            ON n.oid = p.pronamespace
    WHERE
        n.nspname NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND type = 'stored procedure'
        AND {filter_clause}
)
SELECT
    DECODE(ordinal, 1, '', ', ')
    || '{{' || '"ordinal": '  || ordinal::VARCHAR || ''
    || ',"owner":""\" '       || owner        || ' ""\"'
    || ',"database":""\" '    || '{database}' || ' ""\"'
    || ',"schema":""\" '      || schema       || ' ""\"'
    || ',"stored_proc":""\" ' || stored_proc  || ' ""\"'
    || ',"ddl":""\" '         || ddl          || ' ""\"' || '}}' AS data
FROM
    stored_proc_describe
ORDER BY LOWER(schema), LOWER(stored_proc)
"""

    config = {'output_tmplt_default': '{ddl}{^M}' }

    def init_config(self, object_type):
        """Initialize config dict.
        """
        cmd_line_args = {
            'sequence' : "@$HOME/conn.args --current_schema dev --sequence_like '%id%' --"
            , 'stored_proc' : "@$HOME/conn.args --current_schema dev --stored_proc_like '%id%' --"
            , 'table' : "@$HOME/conn.args --current_schema dev  --table_like 'sale_%' --"
            , 'view' : "@$HOME/conn.args --schema_in dev Prod --with_db --view_like '%sale%' --"
        }
        self.config['description'] = ('Return the {type}/s DDL for the requested'
                ' database.  Use {type} filters to limit the set'
                ' of tables returned.').format(type = object_type)
        self.config['optional_args_multi'] = ['owner', 'schema', object_type]
        self.config['usage_example'] = {
                'cmd_line_args': cmd_line_args[object_type]
                , 'file_args': [Util.conn_args_file] }
        self.config['output_tmplt_vars'] = []
        if object_type == 'table':
            self.config['output_tmplt_vars'].append('rowcount')
        self.config['output_tmplt_vars'].extend(['%s_path' % object_type
            , 'schema_path', 'ddl', 'ordinal'
            , object_type, 'schema', 'database', 'owner'])

        self.object_type = object_type

    def init(self, object_type, db_conn=None, args_handler=None):
        """Initialize ddl_object class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        self.init_config(object_type)
        self.init_default(db_conn, args_handler)

    def additional_args(self):
        args_ddl_grp = self.args_handler.args_parser.add_argument_group('optional DDL arguments')
        args_ddl_grp.add_argument("--with_schema"
            , action='store_true', help="add the schema name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--with_db"
            , action='store_true', help="add the database name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--new_schema_name"
            , help="set a new schema name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--new_db_name"
            , help="set a new database name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--database", help=argparse.SUPPRESS)
        if self.object_type in ('stored_proc', 'view'):
            args_ddl_grp.add_argument("--or_replace"
                , action="store_true", help="add the 'OR REPLACE' clause to the %s DDL" % self.object_type)

    def additional_args_process(self):
        if self.args_handler.args.new_schema_name:
            self.args_handler.args.with_schema = True
        if self.args_handler.args.new_db_name:
            self.args_handler.args.with_db = True

    def execute(self):
        self.args_handler.args.database = self.db_conn.database
        describe_sql = self.get_describe_sql()
        output = self.exec_query_and_apply_template(describe_sql)

        if output != '':
            output = self.ddl_modifications(
                output, self.args_handler.args)

            if self.args_handler.args.exec_output:
                self.cmd_result = self.db_conn.ybsql_query(output)
                self.cmd_result.on_error_exit()
                output = self.cmd_result.stdout

        return output

    def object_meta_data_to_ybsql_py_dict(self, meta_data):
        # 'object_path|ordinal|owner|database|schema|object'
        ybsql_py_key_values = []

        ybsql_py_key_values.append(self.sql_to_ybsql_py_key_value('ddl'
            , 'DESCRIBE %s ONLY DDL;' % meta_data[0] ) )

        if (self.object_type == 'table'
            and re.search(r'\{rowcount[\}\:]', self.args_handler.args.template) ):
            ybsql_py_key_values.append(self.sql_to_ybsql_py_key_value('rowcount'
                , 'SELECT COUNT(*) FROM %s;' % meta_data[0] ) )

        ybsql_py_key_values.extend(
            self.dict_to_ybsql_py_key_values(
                { 'ordinal':            meta_data[1]
                    , 'owner':          meta_data[2]
                    , 'database':       meta_data[3]
                    , 'schema':         meta_data[4]
                    , self.object_type: meta_data[5] } ) )

        py_dict = self.ybsql_py_key_values_to_py_dict(ybsql_py_key_values)
        return py_dict

    def get_describe_sql(self):
        """Build up SQL DESCRIBE statement/s.

        :return: A string containing the SQL DESCRIBE statement
        """
        if self.object_type == 'stored_proc':
            self.db_filter_args.schema_set_all_if_none()
            filter_clause = self.db_filter_args.build_sql_filter(
                {'schema':'schema', 'stored_proc':'stored_proc', 'owner':'owner'} )

            describe_sql = ddl_object.stored_proc_describe_query.format(
                filter_clause = filter_clause
                , database = self.db_conn.database)
        else:
            args_handler = copy.deepcopy(self.args_handler)
            args_handler.args.exec_output = False
            orig_template = args_handler.args.template
            args_handler.args.template = ('{%s_path}|{ordinal}|{owner}|{database}|{schema}|{%s}'
                % (self.object_type, self.object_type))
            args_handler.config['required_args_single'].append('database')
            code = ('get_{object_type}_names'
                '(db_conn=self.db_conn, args_handler=args_handler)').format(
                    object_type=self.object_type)
            gons = eval(code)

            object_meta_data_rows = gons.execute()

            # I needed to add this as the deepcopy seems to carry over some pointers
            args_handler.args.template = orig_template

            describe_objects = []
            if object_meta_data_rows.strip() != '':
                for object_meta_data in object_meta_data_rows.strip().split('\n'):
                    describe_clause = self.object_meta_data_to_ybsql_py_dict(object_meta_data.split('|'))
                    describe_objects.append(describe_clause)
            describe_sql = '\echo ,\n'.join(describe_objects)

        return describe_sql

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
                        ( args.new_schema_name
                          if args.new_schema_name
                          else ddl_schema)
                        + '.' + tablepath
                    )
                if args.with_db:
                    tablepath = (
                        ( args.new_db_name
                          if args.new_db_name
                          else self.db_conn.database)
                        + '.' + tablepath
                    )
                tablepath = Common.quote_object_paths(tablepath)
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
                line = re.sub(r"( )" + data_type + r"(,?$|\()",
                    r"\1%s\2" % data_type.upper(), line)

            new_ddl.append(line)

        new_ddl = '\n'.join(new_ddl).strip() + '\n'

        if self.object_type in('stored_proc', 'view') and self.args_handler.args.or_replace:
            typ = {'view':'VIEW','stored_proc':'PROCEDURE'}[self.object_type]
            new_ddl = new_ddl.replace('CREATE %s'%typ, 'CREATE OR REPLACE %s'%typ)

        #remove DDL comments at the beginning of each object definition
        new_ddl = re.sub(r"--( |-).*?\n", "", new_ddl)
        #correct trailing ';' at end of each definition to be consistent
        new_ddl = re.sub(r"(\s*);", ";", new_ddl)

        return new_ddl


def main(util_name):
    ddlo = ddl_object(util_name=util_name, init_default=False)
    ddlo.init(object_type=util_name[4:])
    
    print(ddlo.execute())

    exit(ddlo.cmd_result.exit_code)