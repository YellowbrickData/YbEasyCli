#!/usr/bin/env python3
"""
USAGE:
      yb_get_stored_proc_names.py [options]

PURPOSE:
      List the stored procedure names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_stored_proc_names.py --help)

Output:
      The names of all stored procedures will be listed out, one per line.
"""
import sys
from yb_common import Util

class get_stored_proc_names(Util):
    """Issue the ybsql command to list the stored procedures found in a particular database.
    """
    config = {
        'description': 'List/Verifies that the specified stored procedure/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'database', 'schema', 'stored_proc']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --stored_proc_like '%price%' --stored_proc_NOTlike '%id%' --"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['stored_proc_path', 'schema_path', 'stored_proc', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{stored_proc_path}'
        , 'db_filter_args': {'owner':'u.name', 'database':'d.name', 'schema':'s.name', 'stored_proc':'sp.name'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = ''
        for db in self.get_dbs():
            sql_query += '\\c %s' % db

            sql_query += """
WITH
d AS (
    SELECT database_id, name FROM sys.database WHERE name = CURRENT_DATABASE()
),
sp AS (
    SELECT
        proowner AS owner_id, proname AS name, pronamespace AS schema_id
        , CASE
            WHEN proisagg THEN 'agg'
            WHEN proiswindow THEN 'window'
            WHEN prosp THEN 'stored procedure'
            WHEN prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
                ELSE 'normal'
        END AS TYPE, *
    FROM
        pg_catalog.pg_proc
    WHERE
        type = 'stored procedure'
)
, data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(d.name), LOWER(s.name), LOWER(sp.name)) AS ordinal
        , '{{'
        || '"owner":""\" '        || NVL(u.name, '<NULL>')  || ' ""\"'
        || ',"database":""\" '    || NVL(d.name, '<NULL>')  || ' ""\"'
        || ',"schema":""\" '      || NVL(s.name, '<NULL>')  || ' ""\"'
        || ',"stored_proc":""\" ' || NVL(sp.name, '<NULL>') || ' ""\"' || '}}, ' AS data
    FROM
        sp
        CROSS JOIN d
        LEFT JOIN sys.schema AS s
            ON sp.schema_id = s.schema_id AND d.database_id = s.database_id
        LEFT JOIN sys.user AS u
            ON sp.owner_id = u.user_id
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
    gspn = get_stored_proc_names()

    sys.stdout.write(gspn.execute())

    exit(gspn.cmd_result.exit_code)


if __name__ == "__main__":
    main()
