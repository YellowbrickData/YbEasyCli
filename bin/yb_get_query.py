#!/usr/bin/env python3
"""
USAGE:
      yb_get_query.py [options]

PURPOSE:
      Get the SQL of a query by it's query_id, including base64/gzipped SQL.

OPTIONS:
      See the command line help message for all options.
      (yb_get_query.py --help)

Output:
      The SQL for the requested query.
"""
import base64, gzip, sys
from yb_common import Common, Util

class get_query(Util):
    """Get the SQL of a query by it's query_id.
    """
    config = {
        'description': ("Get the SQL of a query by it's query_id, including base64/gzipped SQL."
            '\n'
            '\nnote:'
            '\n  Mainly used to display queries that are stored in the system views as'
            '\n  SQL in base64/gzipped strings.')
        , 'optional_args_single': []
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args --query_id 2234235456'
            , 'file_args': [Util.conn_args_file] } }

    def additional_args(self):
        args_grp = self.args_handler.args_parser.add_argument_group('get query arguments')
        args_grp.add_argument("--query_id", required=True, type=int
            , help='query id of SQL to be retrieved')

    def execute(self):
        if self.db_conn.ybdb['version_major'] >= 5:
            sql_query = """
WITH q AS (
    SELECT plan_id, query_id, query_text FROM sys.query WHERE num_restart = 0 AND query_id = {query_id}
    UNION ALL SELECT plan_id, query_id, query_text FROM sys.log_query WHERE num_restart = 0 AND query_id = {query_id}
)
, q_filter AS (
    SELECT query_text, 1 AS text_index FROM q
    WHERE plan_id NOT IN (SELECT plan_id FROM sys._log_query_text)
)
SELECT query_text FROM q_filter
UNION ALL (SELECT query_text FROM sys._log_query_text WHERE plan_id = (SELECT plan_id FROM q) ORDER BY text_index)
""".format(query_id=self.args_handler.args.query_id)
        else:
            sql_query = """
SELECT plan_id, query_id, query_text FROM sys.query WHERE query_id = {query_id}
""".format(query_id=self.args_handler.args.query_id)

        cmd_results = self.db_conn.ybsql_query(sql_query)
        cmd_results.on_error_exit()

        if (cmd_results.stdout.strip() == ''):
            Common.error(('query_id(%d) not found' % self.args_handler.args.query_id), color='yellow')
        else:
            if (cmd_results.stdout[0:4] == 'G64:'):
                #print(zlib.decompress(base64.b64decode(cmd_results.stdout[4:]), -1))
                #print(gzip.GzipFile(fileobj=io.StringIO(base64.b64decode(cmd_results.stdout[4:]))).read())
                if (sys.version_info.major == 2):
                    import StringIO
                    print(gzip.GzipFile(fileobj=StringIO.StringIO(base64.b64decode(cmd_results.stdout[4:]))).read())
                else:
                    print(gzip.decompress(base64.b64decode(cmd_results.stdout[4:])).decode('utf-8'))
            else:
                print(cmd_results.stdout)
        
def main():
    get_query().execute()


if __name__ == "__main__":
    main()