#!/usr/bin/env python3

from yb_common import Common, Report, StoredProc, Text, Util

class SPReportUtil(Util):
    sysviews_db = 'sysviews'

    def __init__(self, db_conn=None, args_handler=None, init_default=True, util_name=None, strip_warnings=[]):
        self.strip_warnings=strip_warnings
        self.config['optional_args_single'] = []
        self.config['report_columns'] = 'get_post_db_conn'

        super(SPReportUtil, self).__init__(db_conn, args_handler, init_default, util_name)

        version = 4
        if (self.args_handler.args.sql_only or self.db_conn.ybdb['version_major'] > 4):
            version = 5

        full_proc_name = '{location}_yb{version}/{proc_name}'.format(
            location=self.config['report_sp_location']
            , version=version
            , proc_name= ((self.__class__.__name__).replace('report_', '') + '_p') )
        if StoredProc.proc_file_exists(full_proc_name):
            self.sp = StoredProc(full_proc_name)
        else:
            Common.error('is not implemented for YBDB version: %d' % self.db_conn.ybdb['version_major'])

        self.config['report_columns'] = '|'.join(self.sp.row_cols)
        self.args_handler.process_report_args()

        self.select_columns = ', '.join(Common.qa(self.config['report_columns']))
        self.db_filter_args.schema_set_all_if_none()
        self.order_by_clause = (self.args_handler.args.report_order_by
            if self.args_handler.args.report_order_by != ''
            else '')

    def build_for_su(self, args, where_clause):
        (new_table_name, anonymous_pl) = self.sp.proc_setof_to_anonymous_block(args)

        where_clause = ((' WHERE %s' % where_clause) if where_clause else '')

        report_query = ('SELECT %s FROM %s%s'
            % (self.select_columns, new_table_name, where_clause) )

        return Report(
            self.args_handler, self.db_conn
            , self.config['report_columns']
            , report_query
            , pre_sql=anonymous_pl
            , order_by=self.order_by_clause
            , strip_warnings=self.strip_warnings).build(is_source_cstore=True)

    def get_create_table(self):
        self.sp.parse_setof_create_table(new_table_name=self.args_handler.args.report_dst_table)
        cols = []
        if self.args_handler.args.report_add_ts_column:
            cols.append('at TIMESTAMP WITHOUT TIME ZONE')
        for col_name in self.config['report_columns']:
            #cols.append(next(item for item in self.sp.column_definitions if item["name"] == col)['def'])
            cols.append(self.sp.row_cols_def[col_name]['def'])

        return 'CREATE TABLE {table} (\n    {cols}\n) DISTRIBUTE RANDOM'.format(
                table=self.args_handler.args.report_dst_table
                , cols=('\n    , '.join(cols) ) )

    #TODO much of the build_for_non_su logic might be more appropriate in the Report class
    def build_for_non_su(self, args, where_clause):
        pre_conn_db = self.db_conn.env['conn_db']
        self.db_conn.env['conn_db'] = self.sysviews_db
        if Common.verbose >= 3:
            print('%s: %s' % (Text.color('Using sysviews procs in db', 'cyan')
                , Text.color(self.sysviews_db, style='bold') ) )

        proc_priv_query = ("SELECT HAS_FUNCTION_PRIVILEGE('%s', '%s','EXECUTE')"
            % (self.db_conn.ybdb['user'], self.sp.get_proc_declaration()) )
        result = self.db_conn.ybsql_query(proc_priv_query)
        if result.stdout.strip() != 't':
            Common.error('this report may only be run by a DB super user'
                '\nor you may ask your DBA to perform the non-super user prerequisites'
                '\nwhich require installing the sysviews library and granting permissions')

        args_clause = self.sp.input_args_to_args_clause(args, is_declare=False)
        where_clause = ((' WHERE %s' % where_clause) if where_clause else '')

        report_query = ('SELECT {at}{columns} FROM {proc}({args}){where}'.format(
            at=('LOCALTIMESTAMP AS "at", ' if self.args_handler.args.report_add_ts_column else '')
            , columns=self.select_columns
            , proc=self.sp.proc_name
            , args=args_clause
            , where=where_clause) )

        report_type = self.args_handler.args.report_type
        if report_type in ('ctas', 'insert'):
            self.args_handler.args.report_type = 'psv'

        report = Report(
            self.args_handler, self.db_conn
            , self.config['report_columns']
            , report_query
            , order_by=self.order_by_clause
            , strip_warnings=self.strip_warnings).build()
        self.db_conn.env['conn_db'] = pre_conn_db

        if report_type in ('ctas', 'insert'):
            if report_type == 'ctas':
                result = self.db_conn.ybsql_query(self.get_create_table())
                result.on_error_exit()
                
            report = ''.join(report.splitlines(keepends=True)[1:]) #strip header from report
            cmd = ("""ybsql -A -q -t -v ON_ERROR_STOP=1 -X 'host=%s connect_timeout=10' -c "\copy %s FROM STDIN DELIMITER '|'" """
                % (self.db_conn.env['host'], self.args_handler.args.report_dst_table) )
            result = self.db_conn.ybtool_cmd(cmd, stdin=report)
            result.on_error_exit()

            report = '--Report type "%s" completed' % report_type

        return report

    def build(self, args={}, where_clause=None):
        if self.db_conn.ybdb['is_super_user']:
            report = self.build_for_su(args, where_clause)
        else:
            report = self.build_for_non_su(args, where_clause)
        
        return report