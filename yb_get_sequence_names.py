#!/usr/bin/env python3
"""
USAGE:
      yb_get_sequence_names.py [options]
PURPOSE:
      List the sequence names found in this database.
OPTIONS:
      See the command line help message for all options.
      (yb_get_sequence_names.py --help)
Output:
      The names of all sequences will be listed out, one per line.
"""
import sys
from yb_common import Util

class get_sequence_names(Util):
    """Issue the ybsql command to list the sequences found in a particular
    database.
    """
    config = {
        'description': 'List/Verifies that the specified sequence/s exist.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'sequence']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --sequence_like '%price%' --sequence_NOTlike '%id%' --"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '{sequence_path}', 'exec_output': False}
        , 'output_tmplt_vars': ['sequence_path', 'schema_path', 'sequence', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{sequence_path}'
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'sequence':'seq.name'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = ''
        for db in self.get_dbs():
            sql_query += '\\c %s' % db

            sql_query += """
WITH
seq AS (
    SELECT
        seq.database_id, seq.schema_id, cls.relname AS name, cls.relowner AS owner_id
    FROM
        sys.sequence AS seq
        LEFT JOIN pg_catalog.pg_class AS cls
            ON seq.sequence_id = cls.oid AND cls.relkind IN ('S')
)
, data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(d.name), LOWER(s.name), LOWER(seq.name)) AS ordinal
        , '{{'
        || '"owner":""\" '     || NVL(u.name, '<NULL>')   || ' ""\"'
        || ',"database":""\" ' || NVL(d.name, '<NULL>')   || ' ""\"'
        || ',"schema":""\" '   || NVL(s.name, '<NULL>')   || ' ""\"'
        || ',"sequence":""\" ' || NVL(seq.name, '<NULL>') || ' ""\"' || '}}, ' AS data
    FROM
        seq
        LEFT JOIN sys.schema AS s
            ON seq.schema_id = s.schema_id AND seq.database_id = s.database_id
        LEFT JOIN sys.database AS d
            ON seq.database_id = d.database_id
        LEFT JOIN sys.user AS u
            ON seq.owner_id = u.user_id
    WHERE
        s.name NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND {filter_clause}
)
SELECT data FROM data ORDER BY ordinal;\n""".format(
                filter_clause = self.db_filter_sql() )

        self.cmd_result = self.db_conn.ybsql_query(sql_query)
        self.cmd_result.on_error_exit()

        data = ''
        ordinal = 1
        for line in self.cmd_result.stdout.splitlines():
            data += line.replace('{', '{"ordinal":""\" %d ""\", ' % ordinal) + '\n'
            ordinal += 1

        return self.apply_template(data, exec_output=self.args_handler.args.exec_output)


def main():
    gsn = get_sequence_names()
    
    sys.stdout.write(gsn.execute())

    exit(gsn.cmd_result.exit_code)


if __name__ == "__main__":
    main()