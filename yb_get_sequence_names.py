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

import sys

import yb_common


class get_sequence_names:
    """Issue the ybsql command to list the sequences found in a particular
    database.
    """

    def __init__(self, common=None, db_args=None):
        """Initialize get_sequence_names class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.
        """
        if common:
            self.common = common
            self.db_args = db_args
        else:
            self.common = yb_common.common()

            self.db_args = self.common.db_args(
                description=
                    'List/Verifies that the specified sequence/s exist.',
                optional_args_multi=['sequence', 'owner', 'schema'])

            self.common.args_process()

        self.db_conn = yb_common.db_connect(self.common.args)

    def execute(self):
        filter_clause = self.db_args.build_sql_filter({
            'owner':'sequenceowner'
            , 'schema':'schemaname'
            , 'sequence':'sequencename'})

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
    schemaname || '.' || sequencename
FROM
    objct
WHERE
    {filter_clause}
ORDER BY LOWER(schemaname), LOWER(sequencename)""".format(
             filter_clause = filter_clause
             , database_name = self.db_conn.database)

        self.cmd_results = self.db_conn.ybsql_query(sql_query)


def main():
    gsn = get_sequence_names()
    gsn.execute()

    gsn.cmd_results.write(quote=True)

    exit(gsn.cmd_results.exit_code)


if __name__ == "__main__":
    main()
