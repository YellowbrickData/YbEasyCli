#!/usr/bin/env python3
"""
USAGE:
      yb_get_stored_proc_names.py [database] [options]

PURPOSE:
      List the stored procedure names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_stored_proc_names.py --help)

Output:
      The names of all stored procedures will be listed out, one per line.
"""
from yb_util import util

class get_stored_proc_names(util):
    """Issue the ybsql command to list the stored procedures found in a particular database.
    """
    config = {
        'description': 'List/Verifies that the specified stored procedure/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'stored_proc']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --stored_proc_like '%price%' --stored_proc_NOTlike '%id%' --"
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['stored_proc_path', 'schema_path', 'stored_proc', 'schema', 'database']
        , 'output_tmplt_default': '<stored_proc_path>'
        , 'db_filter_args': {'owner':'owner', 'schema':'schema', 'stored_proc':'stored_proc'} }

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
WITH
objct AS (
    SELECT
        n.nspname AS schema
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
        n.nspname NOT IN ('sys', 'pg_catalog', 'information_schema')
        AND type = 'stored procedure'
)
SELECT
    '{database}.' || schema || '.' || stored_proc
FROM
    objct
WHERE
    schema NOT IN ('sys', 'pg_catalog', 'information_schema')
    AND {filter_clause}
ORDER BY LOWER(schema), LOWER(stored_proc)""".format(
             filter_clause = filter_clause
             , database = self.db_conn.database)

        self.exec_query_and_apply_template(sql_query)

def main():
    gspn = get_stored_proc_names()
    gspn.execute()

    gspn.cmd_results.write()

    exit(gspn.cmd_results.exit_code)


if __name__ == "__main__":
    main()
