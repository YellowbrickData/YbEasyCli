#!/usr/bin/env python3
"""
USAGE:
      yb_convert_distribution.py [options]
PURPOSE:
      Convert tables to REPLICATE or RANDOM distribution based on storage size.
OPTIONS:
      See the command line help message for all options.
      (yb_convert_distribution.py --help)

Output:
      SQL Script to perform table/s distribution converstion.
"""
import copy
import re

from yb_common import Common, Util
from yb_ddl_object import ddl_object

class convert_table_to_dist_replicate(Util):
    """Convert tables to REPLICATE or RANDOM distribution based on storage size.
    """
    config = {
        'description': 'Convert tables to REPLICATE or RANDOM distribution based on storage size.'
        , 'optional_args_multi': ['owner', 'schema', 'table']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --table_like '%dim%'"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '{table_path}', 'exec_output': True}
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'table':'t.name'} }

    def additional_args(self):
        table_attribute_grp = self.args_handler.args_parser.add_argument_group('table attribute arguments')
        table_attribute_grp.add_argument(
            "--distribute", choices=('RANDOM', 'REPLICATE'), required=True, help='convert table to distribute random or replicate')
        table_attribute_grp.add_argument(
            "--min_storage_size_mb", type=int, help='min table storage size in MB to convert, default for REPLICATE is 0MB, default for RANDOM is 1001MB', default=None)
        table_attribute_grp.add_argument(
            "--max_storage_size_mb", type=int, help='max table storage size in MB to convert, default for REPLICATE is 1000MB, default for RANDOM is 100000MB', default=None)
        table_attribute_grp.add_argument(
            "--exec_conversion", action="store_true"
            , help="execute table conversion SQL, defaults to FALSE")

    def additional_args_process(self):
        if self.args_handler.args.min_storage_size_mb is None:
            self.args_handler.args.min_storage_size_mb = (
                0 if self.args_handler.args.distribute == 'REPLICATE' else 1001)
        if self.args_handler.args.max_storage_size_mb is None:
            self.args_handler.args.max_storage_size_mb = (
                1000 if self.args_handler.args.distribute == 'REPLICATE' else 100000)

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        ddl = self.table_ddl(self.db_conn, self.args_handler)

        distribute = ('random' if self.args_handler.args.distribute == 'RANDOM' else 'replicated')

        sql_query = """
SELECT
    u.name
    , d.name
    , s.name
    , t.name
    , d.name || '.' || s.name || '.' || t.name
    , t.distribution
    , ((t.compressed_bytes / (10^6))::BIGINT)::VARCHAR AS mb
FROM
    sys.table AS t
    LEFT JOIN sys.schema AS s
        ON t.schema_id = s.schema_id AND t.database_id = s.database_id
    LEFT JOIN sys.database AS d
        ON t.database_id = d.database_id
    LEFT JOIN sys.user AS u
        ON t.owner_id = u.user_id
WHERE
    s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND t.distribution != '{distribute}'
    AND {filter_clause}
    AND d.name = '{dbname}'
    AND mb BETWEEN {min_storage_size_mb} AND {max_storage_size_mb}
ORDER BY LOWER(d.name), LOWER(s.name), LOWER(t.name);\n""".format(
            filter_clause = self.db_filter_sql()
            , distribute          = distribute
            , min_storage_size_mb = self.args_handler.args.min_storage_size_mb
            , max_storage_size_mb = self.args_handler.args.max_storage_size_mb
            , dbname              = self.db_conn.database )

        self.cmd_result = self.db_conn.ybsql_query(sql_query)
        self.cmd_result.on_error_exit()

        sql_query = ''
        for table_row in self.cmd_result.stdout.splitlines():
            table_path = Common.quote_object_paths(table_row.split('|')[4])
            table_mb = table_row.split('|')[6]
            table_backup_path = (table_path[:-1] + '__old"' if table_path[-1] == '"' else table_path + '__old')
            table_backup = table_backup_path.split('.')[2]

            re_table = re.escape(table_path).replace('"', '\\"')
            re_ddl = r"(CREATE\s+((\b[^(]*\b)*)\s+)(" + re_table + r"([^;]*))(DISTRIBUTE\s+ON\s+[^\)]*\)|DISTRIBUTE\s+RANDOM|DISTRIBUTE\s+ON\s+RANDOM|DISTRIBUTE\s+REPLICATE|DISTRIBUTE\s+ON\s+REPLICATE)([^;]*);"
            match = re.search(re_ddl, ddl, re.MULTILINE)
            create_table = match.group(1) + match.group(4) + 'DISTRIBUTE ' + self.args_handler.args.distribute + match.group(7);

            sql_query += """
----------------------
-- Table: {table_path}, Storage: {table_mb}MB, Distribute {distribute} Convertion
----------------------
BEGIN;
ALTER TABLE {table_path} RENAME TO {table_backup};
{create_table};
INSERT INTO {table_path} SELECT * FROM {table_backup_path};
DROP TABLE {table_backup_path};
COMMIT;\n""".format(
                table_path          = table_path
                , table_backup      = table_backup
                , table_backup_path = table_backup_path
                , table_mb          = table_mb
                , create_table      = create_table
                , distribute        = self.args_handler.args.distribute)

        if self.args_handler.args.exec_conversion:
            cmd_result = self.db_conn.ybsql_query(sql_query, options = '-A -q -t -e -v ON_ERROR_STOP=1 -X')
            cmd_result.on_error_exit()
            return(cmd_result.stdout)
        else:
            return(sql_query)

    def table_ddl(self, db_conn, in_args_handler):
        args_handler = copy.deepcopy(in_args_handler)
        db_conn = copy.deepcopy(db_conn)

        ddlo = ddl_object(db_conn=db_conn, args_handler=args_handler)

        ddlo.init_config('table')
        args_handler.args.template        = '{ddl}'
        args_handler.args.with_schema     = False
        args_handler.args.with_db         = True
        args_handler.args.exec_output     = False
        args_handler.args.new_schema_name = None
        args_handler.args.new_db_name     = None

        return(ddlo.execute())

def main():
    ctdr = convert_table_to_dist_replicate()
    
    print(ctdr.execute())

    exit(ctdr.cmd_result.exit_code)


if __name__ == "__main__":
    main()
