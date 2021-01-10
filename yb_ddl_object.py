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

    stored_proc_describe_query = """WITH
stored_proc_describe AS (
    SELECT
        n.nspname AS schema
        , p.proname AS stored_proc
        , pg_catalog.pg_get_userbyid(p.proowner) AS owner
        , pg_catalog.pg_get_functiondef(p.oid) AS ddl
        , CASE
            WHEN p.proisagg THEN 'agg'
            WHEN p.proiswindow THEN 'window'
            WHEN p.prosp THEN 'stored procedure'
            WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
                ELSE 'normal'
        END AS type
    FROM
        {database}.pg_catalog.pg_proc AS p
        LEFT JOIN {database}.pg_catalog.pg_namespace AS n
            ON n.oid = p.pronamespace
    WHERE
        n.nspname NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND type = 'stored procedure'
)
SELECT
     '-- Schema: ' || schema
    || CHR(10) || 'CREATE PROCEDURE '
    || stored_proc || REPLACE(REGEXP_REPLACE(ddl, '[^(]*', ''), '$function$', '$CODE$')
FROM
    stored_proc_describe
WHERE
    {filter_clause}
ORDER BY LOWER(schema), LOWER(stored_proc)
"""

    config = {'optional_args_single': ['database']}

    def init(self, object_type, db_conn=None, args_handler=None):
        """Initialize ddl_object class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
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
                , 'file_args': [util.conn_args_file] }

        self.object_type = object_type
        self.init_default(db_conn, args_handler)

    def additional_args(self):
        args_ddl_grp = self.args_handler.args_parser.add_argument_group('optional DDL arguments')
        args_ddl_grp.add_argument("--with_schema"
            , action='store_true', help="add the schema name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--with_db"
            , action='store_true', help="add the database name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--schema_name"
            , help="set a new schema name to the %s DDL" % self.object_type)
        args_ddl_grp.add_argument("--db_name"
            , help="set a new database name to the %s DDL" % self.object_type)
        if self.object_type == 'table':
            args_ddl_grp.add_argument("--with_rowcount"
                , action="store_true", help="display the current rowcount")
        elif self.object_type in ('stored_proc', 'view'):
            args_ddl_grp.add_argument("--or_replace"
                , action="store_true", help="add the 'OR REPLACE' clause to the %s DDL" % self.object_type)

    def additional_args_process(self):
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
        if self.object_type == 'stored_proc':
            self.db_filter_args.schema_set_all_if_none()
            filter_clause = self.db_filter_args.build_sql_filter(
                {'schema':'schema', 'stored_proc':'stored_proc', 'owner':'owner'} )

            describe_sql = ddl_object.stored_proc_describe_query.format(
                filter_clause = filter_clause
                , database = self.db_conn.database)
        else:
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
            describe_sql = '\n'.join(describe_objects)

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
    ddlo.execute()

    ddlo.cmd_results.write()

    exit(ddlo.cmd_results.exit_code)