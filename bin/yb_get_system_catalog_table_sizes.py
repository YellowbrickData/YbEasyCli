#!/usr/bin/env python3
"""
USAGE:
      yb_get_system_catalog_table_sizes.py [options]
PURPOSE:
      Calculate system catalog table sizes.
OPTIONS:
      See the command line help message for all options.
      (yb_get_system_catalog_table_sizes.py --help)
"""
import os, sys
from yb_common import Util, Report, Common
from tempfile import mkstemp
# This is strictly for backward compatibility with ancient Python 2.x
from collections import OrderedDict

class get_system_catalog_table_sizes(Util):
    units = 'BKMGTP'
    columns = OrderedDict([
        ('database_id'  , {'value': "CASE WHEN c.relisshared THEN          0 ELSE d.oid     END", 'type': 'BIGINT'}),
        ('database_name', {'value': "CASE WHEN c.relisshared THEN '<global>' ELSE d.datname END", 'type': 'VARCHAR(128)'}),
        ('schema_id'    , {'value': 's.oid'                  , 'type': 'BIGINT'}),
        ('schema_name'  , {'value': 's.nspname'              , 'type': 'VARCHAR(128)'}),
        ('table_id'     , {'value': 'c.oid'                  , 'type': 'BIGINT'}),
        ('table_name'   , {'value': 'c.relname'              , 'type': 'VARCHAR(128)'}),
        ('table_size'   , {'value': 'pg_table_size(c.oid)'   , 'type': 'BIGINT'}),
        ('indexes_size' , {'value': 'pg_indexes_size(c.oid)' , 'type': 'BIGINT'}),
        ('total_size'   , {'value': 'table_size+indexes_size', 'type': 'BIGINT'}),
        ('unit'         , {'value': None                     , 'type': 'CHAR(1)'}),
    ])
    config = {
          'description': 'Shows system catalog table sizes'
        , 'optional_args_multi': ['database', 'schema', 'table']
        , 'db_filter_args': {'schema':'s.nspname', 'table':'c.relname'}
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in Prod --table_in sales --"
            , 'file_args': [Util.conn_args_file] }
        , 'report_columns': '|'.join(columns.keys())
        , 'report_default_order': 'database_name|total_size|DESC|schema_name|table_name'
    }

    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('report arguments')
        args_optional_filter_grp.add_argument("--unit"
            , choices = tuple(self.units), default = 'B'
            , help = "data size unit, defaults to B(bytes)")
        args_optional_filter_grp.add_argument("--total_size_min"
            , type = int, default = 0
            , help = "limit the report by the 'total size' column >= specified amount")
        args_optional_filter_grp.add_argument("--skip_global"
            , action = "store_true"
            , help = "exclude global system catalog shared tables from the report")

    def execute(self):
        self.columns['unit']['value'] = "'{unit}'".format(unit=self.args_handler.args.unit)
        if self.args_handler.args.unit != 'B':
            for key in [tag+'_size' for tag in ('table', 'indexes',)]:
                self.columns[key]['value'] = 'round({value}/1024^{power})'.format(
                      value = self.columns[key]['value']
                    , power = self.units.index(self.args_handler.args.unit))
        self.db_filter_args.schema_set_all_if_none()
        temp_table_name = 'z__syscatalog'
        temp_table_ddl  = "CREATE TEMP TABLE {table} (\n{columns}\n)".format(
              table   = temp_table_name
            , columns = '\n,'.join(['{col} {dtype}'.format(col=k, dtype=v['type']) for k,v in self.columns.items()]))

        # Generate a single ybsql script that connects to all specified databases
        # and runs a local SQL against current system catalog.
        sql = ''
        for rownum, db in enumerate(self.get_dbs()):
            sql += """\n\\c {database}\n
SELECT\n{columns}
FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS s ON s.oid = c.relnamespace
    JOIN pg_catalog.pg_database  AS d ON d.datname = '{database}'
WHERE s.nspname IN ('sys', 'pg_catalog')
    AND c.relkind = 'r'
    AND CASE WHEN {rn} = 0 AND {show_global} THEN TRUE ELSE NOT c.relisshared END
    AND total_size >= {min_size}
    AND {filter_clause};\n""".format(
          database      = db
        , rn            = rownum
        , show_global   = 'FALSE' if self.args_handler.args.skip_global else 'TRUE'
        , columns       = '\n,'.join(['{col} AS {dtype}'.format(col=v['value'], dtype=k) for k,v in self.columns.items()])
        , min_size      = self.args_handler.args.total_size_min
        , filter_clause = self.db_filter_sql())

        self.cmd_result = self.db_conn.ybsql_query(sql)
        self.cmd_result.on_error_exit()
        # Save the output to a temp file for loading it into a temp table (needed for nice reporting)
        tmp_dat_fd, tmp_dat_path = mkstemp(prefix='YbEasyCli_syscat_', suffix='.dat')
        # cygwin whatever...
        tmp_dat_path = Common.os_path(tmp_dat_path)
        with os.fdopen(tmp_dat_fd, 'wb') as tmp:
            tmp.write(self.cmd_result.stdout.strip().encode())
        # Create a pre-SQL step for creating a temp table and populating it with data from the main SELECT
        temp_table_script = """{ddl};\n\\copy {table_name} from {file_name} with (delimiter '|')\n""".format(
              ddl        = temp_table_ddl
            , table_name = temp_table_name
            , file_name  = tmp_dat_path)
        report = Report(self.args_handler, self.db_conn, self.config['report_columns']
            , pre_sql  = temp_table_script
            , query    = 'select * from {tmp}'.format(tmp=temp_table_name)).build(is_source_cstore = True)
        os.remove(tmp_dat_path)
        return report

def main():
    gtns = get_system_catalog_table_sizes()
    print(gtns.execute())

if __name__ == "__main__":
    main()
