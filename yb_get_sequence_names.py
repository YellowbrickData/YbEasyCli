#!/usr/bin/env python3
"""
USAGE:
      yb_get_sequence_names.py [database] [options]

PURPOSE:
      List the sequence names found in this database.

OPTIONS:
      See the command line help message for all options.
      (yb_get_sequence_names.py --help)

Output:
      The names of all sequences will be listed out, one per line.
"""
from yb_util import util

class get_sequence_names(util):
    """Issue the ybsql command to list the sequences found in a particular
    database.
    """
    config = {
        'description': 'List/Verifies that the specified sequence/s exist.'
        , 'optional_args_single': ['database']
        , 'optional_args_multi': ['owner', 'schema', 'sequence']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --sequence_like '%price%' --sequence_NOTlike '%id%' --"
            , 'file_args': [util.conn_args_file] }
        , 'default_args': {'template': '<raw>', 'exec_output': False}
        , 'output_tmplt_vars': ['sequence_path', 'schema_path', 'sequence', 'schema', 'database']
        , 'output_tmplt_default': '<sequence_path>'
        , 'db_filter_args': {'owner':'sequenceowner', 'schema':'schemaname', 'sequence':'sequencename'} }

    def execute(self):
        filter_clause = self.db_filter_args.build_sql_filter(self.config['db_filter_args'])

        sql_query = """
WITH
objct AS (
    SELECT
        c.relname AS sequencename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS sequenceowner
    FROM {database_name}.pg_catalog.pg_class AS c
        LEFT JOIN {database_name}.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
    WHERE
        c.relkind IN ('S')
)
SELECT
    '{database_name}.' || schemaname || '.' || sequencename
FROM
    objct
WHERE
    {filter_clause}
ORDER BY LOWER(schemaname), LOWER(sequencename)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.exec_query_and_apply_template(sql_query)

def main():
    gsn = get_sequence_names()
    gsn.execute()

    gsn.cmd_results.write()

    exit(gsn.cmd_results.exit_code)


if __name__ == "__main__":
    main()
