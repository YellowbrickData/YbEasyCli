/* ****************************************************************************
** table_skew_p()
**
** Table skew report.
**
** Usage:
**   See COMMENT ON FUNCTION text further below.
**
** (c) 2021 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.11.13 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  owner       |database      schema   table_id  tablename      disk_skew_max_pct_of_tbl  disk_skew_avg_pct_of_tbl  gbytes_total
**  -----------  ------------  -------  --------  -------------  ------------------------  ------------------------  ------------
**  yellowbrick  tpcds_qumulo  sf10000  351397    store_sales                      1.2206                    0.7012          1972
**  yellowbrick  tpcds_qumulo  sf10000  351391    catalog_sales                    1.0109                    0.6328          1408
**  ...
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS table_skew_t CASCADE
;

CREATE TABLE table_skew_t
(
    owner                       VARCHAR(128)
    , database                  VARCHAR(128)
    , schema                    VARCHAR(128)
    , table_id                  BIGINT
    , tablename                 VARCHAR(128)
    , distribution              VARCHAR(128)
    , sort_or_clstr             VARCHAR(128)
    , prtn_keys                 VARCHAR(128)
    , disk_skew_max_pct_of_wrkr NUMERIC(20,4)
    , disk_skew_avg_pct_of_wrkr NUMERIC(20,4)
    , disk_skew_max_pct_of_tbl  NUMERIC(20,4)
    , disk_skew_avg_pct_of_tbl  NUMERIC(20,4)
    , row_skew_max_pct_of_tbl   NUMERIC(20,4)
    , row_skew_avg_pct_of_tbl   NUMERIC(20,4)
    , cmprs_ratio               NUMERIC(20,4)
    , rows_total                BIGINT
    , rows_wrkr_avg             BIGINT
    , rows_wrkr_min             BIGINT
    , rows_wrkr_max             BIGINT
    , bytes_total               BIGINT
    , bytes_parity              BIGINT
    , bytes_minus_parity        BIGINT
    , bytes_wrkr_avg            BIGINT
    , bytes_wrkr_min            BIGINT
    , bytes_wrkr_max            BIGINT
    , bytes_total_uncmprs       BIGINT
    , mbytes_total              BIGINT
    , mbytes_parity             BIGINT
    , mbytes_minus_parity       BIGINT
    , mbytes_wrkr_avg           BIGINT
    , mbytes_wrkr_min           BIGINT
    , mbytes_wrkr_max           BIGINT
    , mbytes_total_uncmprs      BIGINT
    , gbytes_total              BIGINT
    , gbytes_parity             BIGINT
    , gbytes_minus_parity       BIGINT
    , gbytes_wrkr_avg           BIGINT
    , gbytes_wrkr_min           BIGINT
    , gbytes_wrkr_max           BIGINT
    , gbytes_total_uncmprs      BIGINT
    , tbytes_total              BIGINT
    , tbytes_parity             BIGINT
    , tbytes_minus_parity       BIGINT
    , tbytes_wrkr_avg           BIGINT
    , tbytes_wrkr_min           BIGINT
    , tbytes_wrkr_max           BIGINT
    , tbytes_total_uncmprs      BIGINT
)
;

DROP PROCEDURE IF EXISTS table_skew_p(VARCHAR);

CREATE PROCEDURE table_skew_p(_yb_util_filter VARCHAR DEFAULT 'TRUE')
   RETURNS SETOF table_skew_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT := '';
   _ret_rec   table_skew_t%ROWTYPE;

   _fn_name   VARCHAR(256) := 'table_skew_p';
   _prev_tags VARCHAR(256) := CURRENT_SETTING('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;

BEGIN  

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags TO ''' || _tags || '''';
   EXECUTE _sql ; 

   _sql := $sql$WITH
clstr AS (
    --standalone cluster query
    WITH
    wrkr AS (
        SELECT
            worker_id
            , COUNT(*)         AS drives
            , SUM(total_bytes) AS wrkr_bytes
            , MAX(chassis_id)  AS chassis_id
            , MAX(drive) + 1   AS drives_per_wrkr
            , MIN(total_bytes) AS bytes_drive_min
            , MAX(total_bytes) AS bytes_drive_max
        FROM
            sys.drive_summary
        WHERE drive IS NOT NULL AND total_bytes IS NOT NULL
        GROUP BY
            worker_id
    )
    , chassis AS (
        SELECT
            COUNT(*)              AS chassis
            , MIN(chassis_wrkrs) AS min_chassis_wrkrs
            , MAX(chassis_wrkrs) AS max_chassis_wrkrs
        FROM (SELECT chassis_id, COUNT(*) AS chassis_wrkrs FROM wrkr GROUP BY chassis_id) as wrkrs
    )
    , clstr AS (
        SELECT
            MAX(chassis)                  AS chassis
            , MIN(min_chassis_wrkrs)      AS min_chassis_wrkrs
            , MAX(max_chassis_wrkrs)      AS max_chassis_wrkrs
            , COUNT(*)                    AS total_wrkrs
            , MAX(drives_per_wrkr)        AS drives_per_wrkr
            , MIN(bytes_drive_min)        AS bytes_drive_min
            , MAX(bytes_drive_max)        AS bytes_drive_max
            , MIN(bytes_drive_min) * MAX(drives_per_wrkr) AS bytes_wrkr_min
            , MAX(bytes_drive_max) * MAX(drives_per_wrkr) AS bytes_wrkr_max
            , ROUND((1.0 - ((MAX(max_chassis_wrkrs) - 2) / MAX(max_chassis_wrkrs)::NUMERIC)) * 100, 5) AS disk_parity_pct
            , ROUND(bytes_wrkr_max * (disk_parity_pct/100.0)) AS bytes_wrkr_parity
            , MAX(scratch_bytes)          AS bytes_wrkr_temp
            , ROUND(bytes_wrkr_temp / (bytes_wrkr_max * 1.0)*100.0, 5) AS chassis_temp_pct
            , bytes_wrkr_min - bytes_wrkr_parity - bytes_wrkr_temp AS bytes_wrkr_data
        FROM
            wrkr
            LEFT JOIN sys.storage USING (worker_id)
            CROSS JOIN chassis
    )
    SELECT * FROM clstr
)
, schema AS (
    SELECT d.name AS database, schema_id, s.name
    FROM sys.database AS d JOIN sys.schema AS s USING (database_id)
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
    ti.*
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
    table_info         AS ti
    JOIN table_storage AS ts
        USING (table_id)
WHERE $sql$ || _yb_util_filter;

   --RAISE INFO '_sql=%', _sql;
   FOR _ret_rec IN EXECUTE( _sql ) 
   LOOP
      RETURN NEXT _ret_rec;
   END LOOP;
  
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;     

END;   
$proc$ 
;
COMMENT ON FUNCTION table_skew_p( VARCHAR ) IS 
'Description:
Table skew report. 

Examples:
  SELECT * FROM table_skew_p( );
  
Version:
. 2021.12.09 - Yellowbrick Technical Support 
'
;