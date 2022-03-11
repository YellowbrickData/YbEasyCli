#!/usr/bin/env python3
"""
USAGE:
      yb_copy_db.py [options]

PURPOSE:
      Create a new development DB based on an existing DB.

OPTIONS:
      See the command line help message for all options.
      (yb_exec_ybtool.py --help)

Output:
      TODO.
"""
import re

from yb_common import Common, Util

class CreateDevDB(Util):
    """Create a new development DB based on an existing DB.
    """

    config = {
        'description': 'Create a new development DB based on an existing DB.'
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/create_rules.args --dst_db my_dev_db --exec_sql'
            , 'file_args': [Util.conn_args_file
                , {'$HOME/create_rules.args': '''--create_rules """[
# -- Copy all tables in public schema matching 'dis%' pattern to public_copy schema:
{'type': 'table', 'as_view': False, 'filter': "--schema_in public --table_like 'dis%'", 'dst_schema': 'public_copy'},
# -- Copy prod1.tab1 table with only a subset of data (filtered on inv_warehouse_sk = 16):
{'type': 'table', 'as_view': False, 'filter': '--table_in tab1 --schema_in prod1', 'data_filter': 'inv_warehouse_sk = 16'},
# -- Create a view on prod2.sample table filtering data on content != 'junk':
{'type': 'table', 'as_view': True , 'filter': '--table_in sample --schema_in prod2', 'data_filter': "content != 'junk'"},
# -- Create view1 view just like in the source but pointing to the target objects
{'type': 'view', 'point_at_src': False, 'filter': '--view_in view1'},
# -- Create view2 view just like in the source and still pointing to the source objects, filtering data on inv_item_sk = 59290
{'type': 'view', 'point_at_src': True , 'filter': '--view_in view2', 'data_filter': 'inv_item_sk = 59290'},
]"""'''}] }
    }
    dst_schemas = []

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('create database arguments')
        args_grp.add_argument("--create_rules", required=True,       help='rules to create the target database')
        args_grp.add_argument("--dst_db",       required=True,       help='the target database name')
        args_grp.add_argument("--dst_db_encoding",  choices=('UTF8', 'LATIN9'), default=None
            , help="set the destination database encoding, defaults to the encoding of the source database")
        args_grp.add_argument("--no_create_db", action="store_true", help="don't create the target database, defaults to FALSE")
        args_grp.add_argument("--exec_sql",     action="store_true", help="execute generated SQL in the target database, defaults to FALSE")

    def get_object_list(self, rule):
        cmd = "'%s/yb_get_%s_names.py' %s" % (Common.util_dir_path, rule['type'], rule['filter'])
        cmd_results = self.db_conn.ybtool_cmd(cmd)
        cmd_results.on_error_exit()
        src_object_paths = (cmd_results.stdout.strip()).splitlines()

        objects = []
        for src_object_path in src_object_paths:
            (db, schema, object) = Common.split(src_object_path, '.')
            dst_schema = (rule['dst_schema'] if ('dst_schema' in rule) else schema)
            dst_object_path = ('{dst_schema}.{object}'.format(dst_schema = dst_schema, object = object))
            if dst_schema not in self.dst_schemas:
                self.dst_schemas.append(dst_schema)
            objects.append({'src_path': src_object_path, 'dst_path': dst_object_path})

        return objects

    def get_object_querys(self, rule, query_type, objects):
        where_clause = (('WHERE %s' % rule['data_filter']) if ('data_filter' in rule) else '')

        sql = []
        for object in objects:
            if query_type == 'INSERT':
                query = 'INSERT INTO %s' % object['dst_path']
            elif query_type == 'CREATE VIEW':
                query = 'CREATE VIEW %s AS' % object['dst_path']
            query = ('{query} SELECT * FROM {src_path} {where_clause};'.format(
                query = query
                , src_path = object['src_path']
                , where_clause = where_clause))
            sql.append(query)

        return '\n\n'.join(sql)

    def view_add_where_clause(self, sql, where_clause):
        matches = re.finditer(r"(CREATE VIEW[^\n]*)\n(.*?);$", sql, re.MULTILINE | re.DOTALL)

        sql = []
        for matchNum, match in enumerate(matches, start=1):
            sql.append("""{create_view}
SELECT * FROM (
{view}
) AS src_v WHERE {where_clause};""".format(
                create_view=match.group(1)
                , view=match.group(2)
                , where_clause=where_clause) )

        return '\n\n'.join(sql)

    def get_objects(self, rule, objects):
        dst_schema_arg = ((' --schema_name %s' % rule['dst_schema']) if ('dst_schema' in rule) else '')
        cmd = ("'%s/yb_ddl_%s.py' %s %s --with_schema" % (Common.util_dir_path, rule['type'], dst_schema_arg, rule['filter']) )
        cmd_results = self.db_conn.ybtool_cmd(cmd)
        cmd_results.on_error_exit()
        sql = cmd_results.stdout

        if rule['type'] == 'table':
            sql += '\n\n' + self.get_object_querys(rule, 'INSERT', objects)
        if (rule['type'] == 'view') and ('data_filter' in rule):
            sql = self.view_add_where_clause(sql, rule['data_filter'])

        return sql

    def get_create_schemas(self):
        sql = []
        for dst_schema in self.dst_schemas:
            if dst_schema not in 'public':
                sql.append('CREATE SCHEMA %s;' % dst_schema)

        return '\n\n'.join(sql)

    def get_table(self, rule, objects):
        if rule['as_view']:
            return self.get_object_querys(rule, 'CREATE VIEW', objects)
        else:
            return self.get_objects(rule, objects)

    def get_view(self, rule, objects):
        if rule['point_at_src']:
            return self.get_object_querys(rule, 'CREATE VIEW', objects)
        else:
            return self.get_objects(rule, objects)

    def execute(self):
        create_rules = eval(self.args_handler.args.create_rules)

        if not create_rules:
            print('-- No rules defined, nothing to do, exiting')
            return

        sql = []
        for rule in create_rules:
            sql.append('-- Processing create rule: %s' % str(rule))
            rule['type'] = rule['type'].lower()
            if 'filter' not in rule:
                rule['filter'] = ''
            objects = self.get_object_list(rule)
            if rule['type'] == 'table':
                sql.append(self.get_table(rule, objects))
            elif rule['type'] == 'view':
                sql.append(self.get_view(rule, objects))
        sql = '\n\n'.join(sql)

        sql = '%s\n\n%s' % (self.get_create_schemas(), sql)

        db_encode = (self.args_handler.args.dst_db_encoding
            if self.args_handler.args.dst_db_encoding
            else self.db_conn.ybdb['database_encoding'] )

        create_db_sql = 'CREATE DATABASE %s ENCODING %s;' % (self.args_handler.args.dst_db, db_encode)
        if self.args_handler.args.exec_sql:
            if not self.args_handler.args.no_create_db:
                # Create target database first ...
                self.cmd_result = self.db_conn.ybsql_query(create_db_sql)
                self.cmd_result.on_error_exit()
            # ... then run all SQLs in the target database
            self.db_conn.env['conn_db'] = self.args_handler.args.dst_db
            self.cmd_result = self.db_conn.ybsql_query(sql)
            self.cmd_result.on_error_exit()
            print(self.cmd_result.stdout)
        else:
            if not self.args_handler.args.no_create_db:
                print(create_db_sql, '\n\n-- !!! The next line is ybsql-specific, it would NOT work in any other Yellowbrick client !!! \n\connect %s\n' % self.args_handler.args.dst_db)
            print(sql)

def main():
    CreateDevDB().execute()
    exit(0)

if __name__ == "__main__":
    main()