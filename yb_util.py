import sys
from datetime import datetime

import yb_common
from yb_common import common

def additional_args(args_parser):
    None

class util:
    conn_args_file = {'$HOME/conn.args': """--host yb14
--dbuser dze
--conn_db stores"""}

    config = {}
    config_default = {
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
        , 'additional_args': None }

    def __init__(self, db_conn=None, args_handler=None, init_default=True, util_name=None):
        if util_name:
            self.util_name = util_name
        else:
            self.util_name = self.__class__.__name__

        for k, v in util.config_default.items():
            if k not in self.config.keys():
                self.config[k] = v

        if init_default:
            self.init_default(db_conn, args_handler)

    def init_default(self, db_conn=None, args_handler=None):
        if db_conn: # util called from code with import
            self.db_conn = db_conn
            self.args_handler = args_handler
            for k, v in self.config['default_args'].items():
                if not(hasattr(self.args_handler.args, k)):
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

    def exec_query_and_apply_template(self, sql_query, quote_default=False):
        self.cmd_results = self.db_conn.ybsql_query(sql_query)
        self.cmd_results.on_error_exit()
        return self.apply_template(self.cmd_results.stdout, quote_default)

    def apply_template(self, output_raw, quote_default=False):
        # convert the SQL from code(of a dictionary) to an evaluated dictionary
        rows = eval('[%s]' % output_raw)

        additional_vars = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            , 'max_ordinal': len(rows)
            , '^M': '\n' }

        output_new = ''
        for row in rows:
            format = {}
            # strip vars and add double quotes to non-lower case db objects
            for k, v in row.items():
                if k in ('column', 'database', 'object', 'owner', 'schema', 'sequence', 'stored_proc', 'table', 'view'):
                    #and quote_default  # TODO is this condition needed
                    #and self.args_handler.args.template == self.config['output_tmplt_default']):
                    format[k] = common.quote_object_paths(v.strip())
                elif type(v) is str:
                    format[k] = v.strip()
                else:
                    format[k] = v
            # build *_path vars like table_path and schema_path
            for var in self.config['output_tmplt_vars']:
                path_var = var.rsplit('_',1)
                if len(path_var) == 2 and path_var[1] == 'path':
                    if path_var[0] in ['object', 'sequence', 'stored_proc', 'table', 'view']:
                        format[var] = '%s.%s.%s' % (format['database'], format['schema'], format[path_var[0]])
                    elif path_var[0] in ('schema'):
                        format[var] = '%s.%s' % (format['database'], format['schema'])
                    elif path_var[0] in ('column'):
                        objct = ('table' if ('table' in format) else 'object')
                        format[var] = '%s.%s.%s.%s' % (format['database'], format['schema'], format[objct], format[path_var[0]])

            format.update(additional_vars)
            format.update(self.db_conn.ybdb)
            try:
                output_new += (self.args_handler.args.template.format(**format)
                    + ('\n'))
            #        + ('' if int(row['ordinal']) == len(rows) else '\n'))
            except KeyError as error:
                common.error('%s template var was not found...' % error)

        if self.args_handler.args.exec_output:
            self.cmd_results = self.db_conn.ybsql_query(output_new)
            self.cmd_results.on_error_exit()
            return self.cmd_results.stdout
        else:
            return output_new

    def additional_args(self):
        None

    def additional_args_process(self):
        None

    def get_dbs(self):
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

        cmd_results.on_error_exit()

        dbs = cmd_results.stdout.strip()
        if dbs == '' and self.db_filter_args.has_optional_args_multi_set('database'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + self.db_conn.database + '"']
        else:
            dbs = dbs.split('\n')

        return dbs

    @staticmethod
    def ybsql_py_key_values_to_py_dict(ybsql_py_key_values):
        return """
\\echo {
%s
\\echo }
""" % '\n\\echo ,\n'.join(ybsql_py_key_values)

    @staticmethod
    def sql_to_ybsql_py_key_value(key, sql):
        return """\\echo "%s": '""\"'
%s
\\echo '""\"'\n""" % (key, sql)

    @staticmethod
    def dict_to_ybsql_py_key_values(dct):
        ybsql_py_key_values = []
        for k, v in dct.items():
            ybsql_py_key_values.append(
                """\\echo "%s": ""\" %s ""\"\n""" % (k,v) )
        return ybsql_py_key_values
