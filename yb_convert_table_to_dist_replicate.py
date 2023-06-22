#!/usr/bin/env python3
"""
USAGE:
      yb_convert_table_to_dist_replicate.py [options]
PURPOSE:
      Convert small size tables from Hash and/or Random Distribution to Replicate Distribution.
OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      SQL Script to perform table/s distribution converstion.
"""
import copy
import re

from yb_common import Common, Util
from yb_ddl_object import ddl_object

class convert_table_to_dist_replicate(Util):
    """Convert small size tables from Hash and Random Distribution to Replicate Distribution.
    """
    config = {
        'description': 'Convert small storage tables from Hash and Random Distribution to Replicate Distribution.'
        , 'optional_args_multi': ['owner', 'schema', 'table']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --table_like '%dim%'"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '{table_path}', 'exec_output': False}
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'table':'t.name'} }

    def additional_args(self):
        table_attribute_grp = self.args_handler.args_parser.add_argument_group('table attribute arguments')
        table_attribute_grp.add_argument(
            "--max_storage_size_mb", type=int, help='max table storage size in MB to convert, defaults to 1000MB', default=1000)

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        ddl = self.table_ddl(self.db_conn, self.args_handler)

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
    AND t.DISTRIBUTION != 'replicated'
    AND {filter_clause}
    AND d.name = '{dbname}'
    AND mb <= {max_storage_size_mb}
ORDER BY LOWER(d.name), LOWER(s.name), LOWER(t.name);\n""".format(
            filter_clause = self.db_filter_sql()
            , max_storage_size_mb = self.args_handler.args.max_storage_size_mb
            , dbname = self.db_conn.database )

        self.cmd_result = self.db_conn.ybsql_query(sql_query)
        self.cmd_result.on_error_exit()

        sql_query = ''
        for table_row in self.cmd_result.stdout.splitlines():
            table_path = Common.quote_object_paths(table_row.split('|')[4])
            table_mb = table_row.split('|')[6]
            table_backup_path = (table_path[:-1] + '__old"' if table_path[-1] == '"' else table_path + '__old')
            table_backup = table_backup_path.split('.')[2]

            re_table = re.escape(table_path).replace('"', '\\"')
            re_ddl = r"(CREATE\s+TABLE\s+)(" + re_table + r"([^;]*))(DISTRIBUTE\s+ON\s+[^\)]*\)|DISTRIBUTE\s+RANDOM|DISTRIBUTE\s+ON\s+RANDOM)([^;]*);"
            match = re.search(re_ddl, ddl, re.MULTILINE)
            create_table_replicate = match.group(1) + match.group(2) + 'DISTRIBUTE REPLICATE' + match.group(5);

            sql_query += """
----------------------
-- Table: {table_path}, Storage: {table_mb}MB, Distribute Replicate Convertion
----------------------
ALTER TABLE {table_path} RENAME TO {table_backup};
{create_table_replicate};
INSERT INTO {table_path} SELECT * FROM {table_backup_path};
DROP TABLE {table_backup_path};\n""".format(
                table_path = table_path
                , table_backup = table_backup
                , table_backup_path = table_backup_path
                , table_mb = table_mb
                , create_table_replicate = create_table_replicate )

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
