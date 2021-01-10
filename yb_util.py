import sys

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
                        if var in ('table_path', 'view_path', 'sequence_path', 'stored_proc_path'):
                            value = common.quote_object_paths('.'.join(line.split('.')[0:3]))
                        elif var == 'schema_path':
                            value = common.quote_object_paths('.'.join(line.split('.')[0:2]))
                        elif var == 'data_type':
                            value = line.split('.')[5]
                        elif var == 'ordinal':
                            value = line.split('.')[4]
                        elif var == 'column':
                            value = line.split('.')[3]
                        elif var in ('table', 'view', 'sequence', 'stored_proc'):
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
            sys.stdout.write(yb_common.text.color(cmd_results.stderr, fg='red'))
            exit(cmd_results.exit_code)

        dbs = cmd_results.stdout.strip()
        if dbs == '' and self.db_filter_args.has_optional_args_multi_set('database'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + self.db_conn.database + '"']
        else:
            dbs = dbs.split('\n')

        return dbs
