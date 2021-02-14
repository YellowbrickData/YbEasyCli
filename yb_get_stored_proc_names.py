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
from yb_common import Util

class get_stored_proc_names(Util):
    """Issue the ybsql command to list the stored procedures found in a particular database.
    """
    config = {
        'description': 'List/Verifies that the specified stored procedure/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'stored_proc']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --stored_proc_like '%price%' --stored_proc_NOTlike '%id%' --"
            , 'file_args': [Util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['stored_proc_path', 'schema_path', 'stored_proc', 'schema', 'database', 'owner']
        , 'output_tmplt_default': '{stored_proc_path}'
        , 'db_filter_args': {'owner':'owner', 'schema':'schema', 'stored_proc':'stored_proc'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        sql_query = """
WITH
objct AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(n.nspname), LOWER(p.proname)) AS ordinal
        , n.nspname AS schema
        , p.proname AS stored_proc
        , pg_catalog.pg_get_userbyid(p.proowner) AS owner
        , CASE
            WHEN p.proisagg THEN 'agg'
            WHEN p.proiswindow THEN 'window'
            WHEN p.prosp THEN 'stored procedure'
            WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
                ELSE 'normal'
        END AS type
    FROM
        {database}.pg_catalog.pg_proc AS p
        LEFT JOIN {database}.pg_catalog.pg_namespace AS n
            ON n.oid = p.pronamespace
    WHERE
        schema NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND type = 'stored procedure'
        AND {filter_clause}
)
SELECT
    DECODE(ordinal, 1, '', ', ')
    || '{{' || '"ordinal": ' || ordinal::VARCHAR || ''
    || ',"owner":""\" '       || owner        || ' ""\"'
    || ',"database":""\" '    || '{database}' || ' ""\"'
    || ',"schema":""\" '      || schema       || ' ""\"'
    || ',"stored_proc":""\" ' || stored_proc  || ' ""\"' || '}}' AS data
FROM objct
ORDER BY ordinal""".format(
             filter_clause = self.db_filter_sql()
             , database    = self.db_conn.database)

        return self.exec_query_and_apply_template(sql_query, exec_output=self.args_handler.args.exec_output)

def main():
    gspn = get_stored_proc_names()

    print(gspn.execute())

    exit(gspn.cmd_result.exit_code)


if __name__ == "__main__":
    main()
