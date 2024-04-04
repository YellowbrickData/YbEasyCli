#!/usr/bin/env python3
"""
USAGE:
      yb_get_foreign_key.py [options]
PURPOSE:
      List foreign key constraints.
OPTIONS:
      See the command line help message for all options.
      (yb_get_table_names.py --help)

Output:
      Foreign key contraints.
"""
import re, sys
from yb_common import Report, Util

class get_foreign_key(Util):
    """Issue the command used to list column foreign key constraints.
    """
    config = {
        'description': 'List column foreign key constraints.'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'table']
        , 'usage_example': {
            'cmd_line_args': "@$HOME/conn.args --schema_in Prod --table_in sales --"
            , 'file_args': [Util.conn_args_file] }
        , 'db_filter_args': {'owner':'owner', 'database':'database', 'schema':'primary_key_schema', 'table':'primary_key_table'} }

    def additional_args(self):
        args_fk_o_grp = self.args_handler.args_parser.add_argument_group('optional foreign key argument')
        args_fk_o_grp.add_argument("--only_ddl", action="store_true", help="print the foreign key DDL only, defaults to False")

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        if self.args_handler.args.only_ddl:
            output_columns = 'alter_query'
            order_by_columns = '1'
        else:
            output_columns = """database
    , foreign_key_schema, foreign_key_table, foreign_key_column
    , primary_key_schema, primary_key_table, primary_key_column
    , owner
    , alter_query"""
            order_by_columns = '1, 2, 3, 4, 5, 6, 7'

        query_template="""
\\c {{db}}
WITH
data AS (
    SELECT
        QUOTE_IDENT(CURRENT_DATABASE()) AS database
        , u.name                        AS owner
        , QUOTE_IDENT(pn.nspname)       AS primary_key_schema
        , QUOTE_IDENT(pc.relname)       AS primary_key_table
        , QUOTE_IDENT(a.attname)        AS primary_key_column
        , QUOTE_IDENT(conname)          AS foreign_key_name
        , QUOTE_IDENT(cn.nspname)       AS foreign_key_schema
        , QUOTE_IDENT(c.relname)        AS foreign_key_table
        , QUOTE_IDENT(af.attname)       AS foreign_key_column
        , 'ALTER TABLE ' || database || '.' || foreign_key_schema || '.' || foreign_key_table
        || ' ADD CONSTRAINT ' || foreign_key_name
        || ' FOREIGN KEY (' || foreign_key_column || ')'
        || ' REFERENCES ' || QUOTE_IDENT(database) || '.' || primary_key_schema || '.' || primary_key_table || '(' || primary_key_column || ')'
        AS alter_query
    FROM
        pg_constraint           AS con
        INNER JOIN pg_namespace AS cn ON cn.oid = con.connamespace
        INNER JOIN pg_attribute AS af ON af.attrelid = con.conrelid AND af.attnum = ANY(con.conkey)
        INNER JOIN pg_class     AS c  ON c.oid = con.conrelid
        INNER JOIN pg_namespace AS pn ON pn.oid = c.relnamespace
        INNER JOIN pg_attribute AS a  ON a.attrelid = con.confrelid AND a.attnum = ANY(con.confkey)
        INNER JOIN pg_class     AS pc ON pc.oid = con.confrelid
        INNER JOIN sys.user     AS u  ON u.user_id = c.relowner
    WHERE
        con.contype = 'f'
        AND foreign_key_schema NOT IN ('sys', 'pg_catalog', 'information_schema')
)
SELECT
    {output_columns}
FROM data
WHERE {{filter_clause}}
ORDER BY {order_by_columns};\n""".format(output_columns=output_columns,order_by_columns=order_by_columns)

        # first query is just to get the headers
        sql_query = '\\t\n' + query_template.format(db='yellowbrick', filter_clause='FALSE') + '\\t\n'

        dbs = self.get_dbs()

        for db in dbs:
            sql_query += query_template.format(
                db = db
                , filter_clause = self.db_filter_sql() )

        cmd_result = self.db_conn.ybsql_query(sql_query)

        data = re.sub(r'\(\d+ rows\)[\r\n]+', '', cmd_result.stdout, 0, re.MULTILINE)

        print(Report(self.args_handler, self.db_conn).del_data_to_formatted_report(data))

def main():
    gfkc = get_foreign_key()
    
    gfkc.execute()

if __name__ == "__main__":
    main()
