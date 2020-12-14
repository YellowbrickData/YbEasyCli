import yb_common
from yb_common import common

def additional_args(args_parser):
    None

class util:
    conn_args_file = {'$HOME/conn.args': """--host yb14
--dbuser dze
--conn_db stores"""}

    configs = {
        'default': {
            'description': None
            , 'required_args_single': []
            , 'optional_args_single': ['schema']
            , 'optional_args_multi': []
            , 'positional_args_usage': None
            , 'default_args': {}
            , 'usage_example': {}
            , 'output_tmplt_vars': None
            , 'output_tmplt_default': None
            , 'db_filter_args': {}
            , 'additional_args': None
        }

        , 'analyze_columns': {
            'description': ("Analyze the data content of a table's columns."
                '\n'
                '\nnote:'
                '\n  estimate level anaylsis requires pg_statistic table read privilege and may only display for super users'
                '\n  count and groups level anaylsis may require large pool access and may not display for super users')
            , 'required_args_single': ['table']
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'column']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --schema_in dev --table sales --column_in store_id price --'
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'column':'columnname'}
        }

        , 'check_db_views': {
            'description': 'Check for broken views.'
            , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --database_in stores'
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'ownername','schema':'schemaname','view':'viewname'}
        }

        , 'chunk_dml_by_date_part': {
            'description': 'Chunk DML by DATE/TIMESTAMP column.'
            , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
            , 'default_args': {'pre_sql': '', 'post_sql': ''}
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_date_part.args --print_chunk_dml'
                , 'file_args': [ conn_args_file
                    , {'$HOME/yb_chunk_dml_by_date_part.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_ts\"\"\"
--column 'sale_ts'
--date_part HOUR
--chunk_rows 100000000"""} ] }
        }

        , 'chunk_dml_by_integer': {
            'description': 'Chunk DML by INTEGER column.'
            , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
            , 'default_args': {'pre_sql': '', 'post_sql': ''}
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_integer.args --print_chunk_dml'
                , 'file_args': [ conn_args_file
                    , {'$HOME/yb_chunk_dml_by_integer.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_id\"\"\"
--column 'sale_id'
--chunk_rows 100000000"""} ] }
        }

        , 'chunk_dml_by_yyyymmdd_integer': {
            'description': 'Chunk DML by YYYYMMDD integer column.'
            , 'optional_args_multi': ['owner', 'database', 'schema', 'view']
            , 'default_args': {'pre_sql': '', 'post_sql': ''}
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_yyyymmdd_integer.args --print_chunk_dml'
                , 'file_args': [ conn_args_file
                    , {'$HOME/yb_chunk_dml_by_yyyymmdd_integer.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_date_int\"\"\"
--column 'sale_date_int'
--chunk_rows 100000000"""} ] }
        }

        , 'chunk_optimal_rows': {
            'description': 'Determine the optimal number of rows per chunk for a table.'
            , 'required_args_single': ['table']
            , 'optional_args_single': ['database', 'schema']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --table dze_db1.dev.sales --schema dev'
                , 'file_args': [conn_args_file] }
        }

        , 'ddl_sequence': {
            'description': 'Return the sequence/s DDL for the requested'
                ' database.  Use sequence filters to limit the set'
                ' of tables returned.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['schema', 'sequence']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --current_schema dev --sequence_like '%id%' --"
                , 'file_args': [conn_args_file] }
        }


        , 'ddl_table': {
            'description': 'Return the table/s DDL for the requested'
                ' database.  Use table filters to limit the set'
                ' of tables returned.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['schema', 'table']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --current_schema dev  --table_like 'sale_%' --"
                , 'file_args': [conn_args_file] }
        }


        , 'ddl_view': {
            'description': 'Return the view/s DDL for the requested'
                ' database.  Use view filters to limit the set'
                ' of tables returned.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['schema', 'view']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --with_db --view_like '%sale%' --"
                , 'file_args': [conn_args_file] }
        }

        , 'find_columns': {
            'description': 'List column names and column attributes for filtered columns.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'table', 'column', 'datatype']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' 'TIME%' --"
                , 'file_args': [conn_args_file] }
            , 'default_args': {'template': '<raw>', 'exec_output': False}
            , 'output_tmplt_vars': ['table_path', 'schema_path', 'column', 'ordinal', 'data_type', 'table', 'schema', 'database']
            , 'output_tmplt_default': '-- Table: <table_path>, Column: <column>, Table Ordinal: <ordinal>, Data Type: <data_type>'
            , 'db_filter_args':
                {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname', 'datatype':'datatype'}
        }

        , 'get_column_name': {
            'description': 'List/Verifies that the specified table/view column name if it exists.'
            , 'required_args_single': ['object', 'column']
            , 'optional_args_single': ['owner', 'database', 'schema', ]
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema dev --object sales --column price --"
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'objectowner', 'schema':'schemaname', 'object':'objectname', 'column':'columnname'}
        }

        , 'get_column_names': {
            'description': 'List/Verifies that the specified column names exist.'
            , 'required_args_single': ['object']
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'column']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema dev -- sales"
                , 'file_args': [conn_args_file] }
            , 'default_args': {'template': '<raw>', 'exec_output': False}
            , 'output_tmplt_vars': ['table_path', 'schema_path', 'column', 'table', 'schema', 'database']
            , 'output_tmplt_default': '<column>'
            , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'object':'objectname', 'column':'columnname'}
        }

        , 'get_column_type': {
            'description': 'Return the data type of the requested column.'
            , 'required_args_single': ['table', 'column']
            , 'optional_args_single': ['owner', 'database', 'schema']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema dev --table sales --column price --"
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname'}
        }

        , 'get_sequence_names': {
            'description': 'List/Verifies that the specified sequence/s exist.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'sequence']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --sequence_like '%price%' --sequence_NOTlike '%id%' --"
                , 'file_args': [conn_args_file] }
            , 'default_args': {'template': '<raw>', 'exec_output': False}
            , 'output_tmplt_vars': ['sequence_path', 'schema_path', 'sequence', 'schema', 'database']
            , 'output_tmplt_default': '<sequence_path>'
            , 'db_filter_args': {'owner':'sequenceowner', 'schema':'schemaname', 'sequence':'sequencename'}
        }

        , 'get_table_distribution_key': {
            'description': 'Identify the distribution column or type (random or replicated) of the requested table.'
            , 'required_args_single': ['table']
            , 'optional_args_single': ['owner', 'database', 'schema']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema Prod --table sales --"
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'ownername','schema':'schemaname','table':'tablename'}
        }

        , 'get_table_name': {
            'description': 'List/Verifies that the specified table exists.'
            , 'required_args_single': ['table']
            , 'optional_args_single': ['owner', 'database', 'schema']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --current_schema dev --table sales --'
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'c.tableowner','schema':'c.schemaname','table':'c.tablename'}
        }

        , 'get_table_names': {
            'description': 'List/Verifies that the specified table/s exist.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'table']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --schema Prod --table sales --"
                , 'file_args': [conn_args_file] }
            , 'default_args': {'template': '<raw>', 'exec_output': False}
            , 'output_tmplt_vars': ['table_path', 'schema_path', 'table', 'schema', 'database']
            , 'output_tmplt_default': '<table_path>'
            , 'db_filter_args': {'owner':'c.tableowner', 'schema':'c.schemaname', 'table':'c.tablename'}
        }

        , 'get_view_name': {
            'description': 'List/Verifies that the specified view exists.'
            , 'required_args_single': ['view']
            , 'optional_args_single': ['owner', 'database', 'schema']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --schema Prod --view sales_v --'
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'}
        }

        , 'get_view_names': {
            'description': 'List/Verifies that the specified view/s exist.'
            , 'optional_args_single': ['database']
            , 'optional_args_multi': ['owner', 'schema', 'view']
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --schema_in dev Prod --'
                , 'file_args': [conn_args_file] }
            , 'default_args': {'template': '<raw>', 'exec_output': False}
            , 'output_tmplt_vars': ['view_path', 'schema_path', 'view', 'schema', 'database']
            , 'output_tmplt_default': '<view_path>'
            , 'db_filter_args': {'owner':'v.viewowner','schema':'v.schemaname','view':'v.viewname'}
        }

        , 'is_cstore_table': {
            'description': 'Determine if a table is stored as a column store table.'
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args --table sys.blade --'
                , 'file_args': [conn_args_file] }
        }

        , 'mass_column_update': {
            'description': (
            	'Update the value of multiple columns.'
                '\n'
                '\nnote:'
                '\n  Mass column updates may cause performance issues due to the change '
                '\n  of how the data is ordered in storage.')
            , 'optional_args_single': []
            , 'optional_args_multi': ['owner', 'schema', 'table', 'column', 'datatype']
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' --update_where_clause \"<columnname> = 'NULL'\" --set_clause NULL --"
                , 'file_args': [conn_args_file] }
            , 'db_filter_args': {'owner':'tableowner', 'schema':'schemaname', 'table':'tablename', 'column':'columnname', 'datatype':'datatype'}
        }

        , 'rstore_query_to_cstore_table': {
            'description': 'Convert row store query to column store table.'
            , 'usage_example': {
                'cmd_line_args': '@$HOME/conn.args @$HOME/sys_schema.args --'
                , 'file_args': [conn_args_file
                    , { '$HOME/sys_schema.args': """--query \"\"\"
SELECT name
FROM sys.schema
\"\"\"
--table 'sys_schema'"""} ] }
        }

        , 'yb_to_yb_copy_table': {
            'description': (
                'Copy a table from a source cluster to a destination cluster.'
                '\n'
                '\nnote:'
                '\n  If the src and dst user password differ use SRC_YBPASSWORD and DST_YBPASSWORD env variables.'
                '\n  For manual password entry unset all env passwords or use the --src_W and --dst_W options.')
            , 'positional_args_usage': None
            , 'usage_example': {
                'cmd_line_args': "@$HOME/conn.args --unload_where_clause 'sale_id BETWEEN 1000 AND 2000' --src_table Prod.sales --dst_table dev.sales --log_dir tmp --"
                , 'file_args': [ {'$HOME/conn.args': """--src_host yb14
--src_dbuser dze
--src_conn_db stores_prod
--dst_host yb89
--dst_dbuser dze
--dst_conn_db stores_dev"""} ] }
        }
    }

    def __init__(self, db_conn=None, args_handler=None, init_default=True, util_name=None):
        self.set_config(util_name)

        if init_default:
            self.init_default(db_conn, args_handler)

    def init_default(self, db_conn=None, args_handler=None):
        if db_conn: # util called from code with import
            self.db_conn = db_conn
            self.args_handler = args_handler
            for k, v in self.config['default_args'].items():
                setattr(self.args_handler.args, k, v)
        else: # util called from the command line
            self.args_handler = yb_common.args_handler(self.config, init_default=False)
            self.config['additional_args'] = getattr(self, 'additional_args')
            self.args_handler.init_default()
            self.args_handler.args_process()
            self.additional_args_process()
            self.db_conn = yb_common.db_connect(self.args_handler.args)
        if hasattr(self.args_handler, 'db_filter_args'):
            self.db_filter_args = self.args_handler.db_filter_args

    def set_config(self, util_name):
        if util_name:
            self.util_name = util_name
        else:
            self.util_name = self.__class__.__name__
        self.config = self.configs[self.util_name]
        for k, v in self.configs['default'].items():
            if k not in self.config.keys():
                self.config[k] = v

    def exec_query_and_apply_template(self, sql_query, quote_default=False):
        self.cmd_results = self.db_conn.ybsql_query(sql_query)
        self.apply_template(quote_default)

    def apply_template(self, quote_default=False):
        if self.cmd_results.stderr == '' and self.cmd_results.exit_code == 0:
            self.cmd_results.stdout = util.run_template(
                self.cmd_results.stdout
                , self.args_handler.args.template, self.config['output_tmplt_vars'])

            if (quote_default
                and self.args_handler.args.template == self.config['output_tmplt_default']):
                self.cmd_results.stdout = common.quote_object_paths(self.cmd_results.stdout)

            if self.args_handler.args.exec_output:
                self.cmd_results = self.db_conn.ybsql_query(self.cmd_results.stdout)

    @staticmethod
    def run_template(input, template, vars):
        output = ''
        vars.append('raw')
        if input:
            for line in input.strip().split('\n'):
                if line[0:2] == '--':
                    out_line = line
                else:
                    out_line = template
                    for var in vars:
                        if var in ('table_path', 'view_path', 'sequence_path'):
                            value = common.quote_object_paths('.'.join(line.split('.')[0:3]))
                        elif var == 'schema_path':
                            value = common.quote_object_paths('.'.join(line.split('.')[0:2]))
                        elif var == 'data_type':
                            value = line.split('.')[5]
                        elif var == 'ordinal':
                            value = line.split('.')[4]
                        elif var == 'column':
                            value = line.split('.')[3]
                        elif var in ('table', 'view', 'sequence'):
                            value = line.split('.')[2]
                        elif var == 'schema':
                            value = line.split('.')[1]
                        elif var == 'database':
                            value = line.split('.')[0]
                        elif var == 'raw':
                            value = line
                        out_line = out_line.replace('<%s>' % var, value)
                output += out_line + '\n'
        return output

    def additional_args(self):
        None

    def additional_args_process(self):
        None

    def get_dbs(self):
        db_ct = 0
        broken_view_ct = 0

        filter_clause = self.db_filter_args.build_sql_filter({'database':'db_name'})

        sql_query = """
SELECT
    name AS db_name
FROM
    sys.database
WHERE
    {filter_clause}
ORDER BY
    name""".format(filter_clause = filter_clause)

        cmd_results = self.db_conn.ybsql_query(sql_query)

        if cmd_results.exit_code != 0:
            sys.stdout.write(text.color(cmd_results.stderr, fg='red'))
            exit(cmd_results.exit_code)

        dbs = cmd_results.stdout.strip()
        if dbs == '' and self.db_filter_args.has_optional_args_multi_set('database'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + self.db_conn.database + '"']
        else:
            dbs = dbs.split('\n')

        return dbs
