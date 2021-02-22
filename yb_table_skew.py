#!/usr/bin/env python3
"""
USAGE:
      yb_table_skew.py [options]

PURPOSE:
      Table skew report.

OPTIONS:
      See the command line help message for all options.
      (yb_find_columns.py --help)

Output:
      The skew report as formatted table, pipe seperted value rows, or inserted into a database table.
"""
import re

from yb_common import Common, Report, Util

class table_skew(Util):
    """Issue the ybsql commands used to generate a table skew report."""
    config = {
        'description': 'Table skew report.'
        , 'report_columns': 'at|owner|database|schema|table_id|tablename|distribution|sort_or_clstr|prtn_keys|disk_skew_max_pct_of_wrkr|disk_skew_avg_pct_of_wrkr|disk_skew_max_pct_of_tbl|disk_skew_avg_pct_of_tbl|row_skew_max_pct_of_tbl|row_skew_avg_pct_of_tbl|cmprs_ratio|rows_total|rows_wrkr_avg|rows_wrkr_min|rows_wrkr_max|bytes_total|bytes_parity|bytes_minus_parity|bytes_wrkr_avg|bytes_wrkr_min|bytes_wrkr_max|bytes_total_uncmprs|mbytes_total|mbytes_parity|mbytes_minus_parity|mbytes_wrkr_avg|mbytes_wrkr_min|mbytes_wrkr_max|mbytes_total_uncmprs|gbytes_total|gbytes_parity|gbytes_minus_parity|gbytes_wrkr_avg|gbytes_wrkr_min|gbytes_wrkr_max|gbytes_total_uncmprs|tbytes_total|tbytes_parity|tbytes_minus_parity|tbytes_wrkr_avg|tbytes_wrkr_min|tbytes_wrkr_max|tbytes_total_uncmprs'
        , 'optional_args_multi': ['owner', 'database', 'schema', 'table']
        , 'db_filter_args': {'owner':'owner', 'schema':'schema', 'table':'tablename', 'database':'database'}
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/skew_report.args'
            , 'file_args': [ Util.conn_args_file
                , {'$HOME/skew_report.args': """--skew_pct_column disk_skew_max_pct_of_wrkr
--skew_pct_min 0.005
--report_include_columns \"\"\"
owner database schema table
disk_skew_max_pct_of_wrkr cmprs_ratio
rows_total
gbytes_total
gbytes_wrkr_min gbytes_wrkr_max
gbytes_minus_parity gbytes_total_uncmprs
\"\"\" """} ] } }

    query_skew = """
WITH
clstr AS (
    {cluster_info_sql}
)
, schema AS (
    {schema_with_db_sql}
)
, table_storage_agg AS (
    /* sys.table_storage is by table and worker */
    SELECT
        table_id
        , ROUND(AVG( rows_columnstore ))::BIGINT AS rows_wrkr_avg
        , MIN( rows_columnstore )                AS rows_wrkr_min
        , MAX( rows_columnstore )                AS rows_wrkr_max
        , SUM( rows_columnstore )                AS rows_total
        , ROUND(AVG( compressed_bytes ))::BIGINT AS bytes_wrkr_avg
        , MIN( compressed_bytes )                AS bytes_wrkr_min
        , MAX( compressed_bytes )                AS bytes_wrkr_max
        , SUM( compressed_bytes )                AS bytes_total
        , SUM( uncompressed_bytes )              AS bytes_total_uncmprs
        , MAX( clstr.bytes_wrkr_min )            AS bytes_wrkr
        , MAX( clstr.disk_parity_pct )           AS disk_parity_pct
    FROM
        sys.table_storage
        CROSS JOIN clstr
    GROUP BY
        table_id
    HAVING
        rows_total > 0
)
, table_storage AS (
    SELECT
        table_id
        , rows_wrkr_avg
        , rows_wrkr_min
        , rows_wrkr_max
        , rows_wrkr_max - rows_wrkr_min AS rows_wrkr_max_skew
        , DECODE(TRUE
            , rows_wrkr_avg - rows_wrkr_min > rows_wrkr_max - rows_wrkr_avg
            , rows_wrkr_avg - rows_wrkr_min
            , rows_wrkr_max - rows_wrkr_avg) AS rows_wrkr_avg_skew
        , rows_total
        , bytes_wrkr_avg
        , bytes_wrkr_min
        , bytes_wrkr_max
        , bytes_wrkr_max - bytes_wrkr_min AS bytes_wrkr_max_skew
        , DECODE(TRUE
            , bytes_wrkr_avg - bytes_wrkr_min > bytes_wrkr_max - bytes_wrkr_avg
            , bytes_wrkr_avg - bytes_wrkr_min
            , bytes_wrkr_max - bytes_wrkr_avg) AS bytes_wrkr_avg_skew
        , bytes_total
        , ROUND(bytes_total * disk_parity_pct/100.0)::BIGINT AS bytes_parity
        , bytes_total - bytes_parity AS bytes_minus_parity
        , bytes_total_uncmprs
        , bytes_wrkr
    FROM
        table_storage_agg
)
, table_info AS (
    SELECT
        trim( u.usename::varchar( 128 ) )                         AS owner
        , d.name                                                  AS database
        , s.name                                                  AS schema
        , t.table_id                                              AS table_id
        , t.name                                                  AS tablename
        , CASE
            WHEN t.distribution <> 'hash'
                THEN t.distribution
                ELSE t.distribution
                    || '(' || t.distribution_key || ')'
        END                                                       AS distribution
        , CASE
            WHEN t.sort_key IS NOT NULL AND TRIM(t.sort_key) != ''
                THEN 'sort(' || t.sort_key || ')'
            WHEN t.cluster_keys IS NOT NULL AND TRIM(t.cluster_keys) != ''
                THEN 'clstr(' || t.cluster_keys || ')'
                ELSE NULL::varchar
        END                                                       AS sort_or_clstr
        , t.partition_keys                                        AS prtn_keys
    FROM
        sys.table                     AS t
        INNER JOIN sys.database       AS d
            ON t.database_id = d.database_id
        INNER JOIN schema             AS s
            ON t.schema_id = s.schema_id
            AND d.name = s.database
        INNER JOIN pg_catalog.pg_user AS u
            ON t.owner_id = u.usesysid
    WHERE
       t.distribution != 'replicated'
       AND schema NOT IN ('information_schema', 'pg_catalog', 'sys')
)
SELECT
    CURRENT_TIMESTAMP::TIMESTAMP AS at
    , ti.*
    , DECODE(ts.bytes_wrkr, 0, NULL, ROUND(ts.bytes_wrkr_max_skew / ts.bytes_wrkr::NUMERIC * 100, 4) )         AS disk_skew_max_pct_of_wrkr
    , DECODE(ts.bytes_wrkr, 0, NULL, ROUND(ts.bytes_wrkr_avg_skew / ts.bytes_wrkr::NUMERIC * 100, 4) )         AS disk_skew_avg_pct_of_wrkr
    , DECODE(ts.bytes_wrkr_min, 0, NULL, ROUND(ts.bytes_wrkr_max_skew / ts.bytes_wrkr_min::NUMERIC * 100, 4) ) AS disk_skew_max_pct_of_tbl
    , DECODE(ts.bytes_wrkr_avg, 0, NULL, ROUND(ts.bytes_wrkr_avg_skew / ts.bytes_wrkr_avg::NUMERIC * 100, 4) ) AS disk_skew_avg_pct_of_tbl
    , DECODE(ts.rows_wrkr_min, 0, NULL, ROUND(ts.rows_wrkr_max_skew / ts.rows_wrkr_min::NUMERIC * 100, 4) )    AS row_skew_max_pct_of_tbl
    , DECODE(ts.rows_wrkr_avg, 0, NULL, ROUND(ts.rows_wrkr_avg_skew / ts.rows_wrkr_avg::NUMERIC * 100, 4) )    AS row_skew_avg_pct_of_tbl
    , DECODE(ts.bytes_minus_parity, 0, NULL, ROUND(bytes_total_uncmprs / ts.bytes_minus_parity::NUMERIC, 4) )  AS cmprs_ratio
    , ts.rows_total
    , ts.rows_wrkr_avg, ts.rows_wrkr_min, ts.rows_wrkr_max
    , ts.bytes_total, ts.bytes_parity, ts.bytes_minus_parity
    , ts.bytes_wrkr_avg, ts.bytes_wrkr_min, ts.bytes_wrkr_max
    , ts.bytes_total_uncmprs
    , (ts.bytes_total / 1024^2)::BIGINT         AS mbytes_total
    , (ts.bytes_parity / 1024^2)::BIGINT        AS mbytes_parity
    , (ts.bytes_minus_parity / 1024^2)::BIGINT  AS mbytes_minus_parity
    , (ts.bytes_wrkr_avg / 1024^2)::BIGINT      AS mbytes_wrkr_avg
    , (ts.bytes_wrkr_min / 1024^2)::BIGINT      AS mbytes_wrkr_min
    , (ts.bytes_wrkr_max / 1024^2)::BIGINT      AS mbytes_wrkr_max
    , (ts.bytes_total_uncmprs / 1024^2)::BIGINT AS mbytes_total_uncmprs
    , (ts.bytes_total / 1024^3)::BIGINT         AS gbytes_total
    , (ts.bytes_parity / 1024^3)::BIGINT        AS gbytes_parity
    , (ts.bytes_minus_parity / 1024^3)::BIGINT  AS gbytes_minus_parity
    , (ts.bytes_wrkr_avg / 1024^3)::BIGINT      AS gbytes_wrkr_avg
    , (ts.bytes_wrkr_min / 1024^3)::BIGINT      AS gbytes_wrkr_min
    , (ts.bytes_wrkr_max / 1024^3)::BIGINT      AS gbytes_wrkr_max
    , (ts.bytes_total_uncmprs / 1024^3)::BIGINT AS gbytes_total_uncmprs
    , (ts.bytes_total / 1024^4)::BIGINT         AS tbytes_total
    , (ts.bytes_parity / 1024^4)::BIGINT        AS tbytes_parity
    , (ts.bytes_minus_parity / 1024^4)::BIGINT  AS tbytes_minus_parity
    , (ts.bytes_wrkr_avg / 1024^4)::BIGINT      AS tbytes_wrkr_avg
    , (ts.bytes_wrkr_min / 1024^4)::BIGINT      AS tbytes_wrkr_min
    , (ts.bytes_wrkr_max / 1024^4)::BIGINT      AS tbytes_wrkr_max
    , (ts.bytes_total_uncmprs / 1024^4)::BIGINT AS tbytes_total_uncmprs
FROM
    table_info AS ti
    JOIN table_storage AS ts
        USING (table_id)
WHERE
    {filter_clause}
    AND {pct_filter_clause}
"""
    def additional_args(self):
        args_optional_filter_grp = self.args_handler.args_parser.add_argument_group('optional report filter arguments')
        pct_columns = [
            'disk_skew_max_pct_of_wrkr', 'disk_skew_avg_pct_of_wrkr'
            , 'disk_skew_max_pct_of_tbl', 'disk_skew_avg_pct_of_tbl'
            , 'row_skew_max_pct_of_tbl', 'row_skew_avg_pct_of_tbl']
        args_optional_filter_grp.add_argument("--skew_pct_column"
            , choices = pct_columns
            , help="limit the report by the selected skew percent column")
        args_optional_filter_grp.add_argument("--skew_pct_min"
            , type=float
            , help="limit the report by the selected column with the specified minimum percent")

    def additional_args_process(self):
        args = self.args_handler.args
        if (bool(args.skew_pct_column) != bool(args.skew_pct_min)):
            self.args_handler.args_parser.error('both --skew_pct_column and --skew_pct_min must be set')
        elif args.skew_pct_column:
            args.report_sort_column = args.skew_pct_column
            args.report_sort_reverse = True

    def execute(self):
        self.db_filter_args.schema_set_all_if_none()

        if self.args_handler.args.skew_pct_column:
            pct_filter_clause = '%s >= %f' % (
                self.args_handler.args.skew_pct_column
                , self.args_handler.args.skew_pct_min)
        else:
            pct_filter_clause = 'TRUE'

        cluster_info_sql = self.get_cluster_info(return_format='sql')

        report_query = self.query_skew.format(
            schema_with_db_sql = self.schema_with_db_sql()
            , filter_clause = self.db_filter_sql()
            , pct_filter_clause = pct_filter_clause
            , cluster_info_sql = cluster_info_sql)

        return Report(
            self.args_handler, self.db_conn
            , self.config['report_columns']
            , report_query).build()

def main():
    ts = table_skew()
    if not ts.db_conn.ybdb['is_super_user']:
        Common.error('must be run by a database super user...')

    print(ts.execute())

    exit(0)

if __name__ == "__main__":
    main()
