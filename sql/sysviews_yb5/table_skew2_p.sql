/* ****************************************************************************
** table_skew_p()
**
** Table skew by table with worker id
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
** . 2023.06.01 - change ORDER BY in CTE to LIMIT and usage text.
** . 2023.03.09 - Add row_skw, gb_skw, & fullest blade
** . 2022.08.07 - Overlaod of table_skew_p(VARCHAR) to include filter args,
**                , set units to GB, show only skew across blades.
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.11.13 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  db_name | table_id | schema_name | table_name | table_owner | cols | distribution |   sort_or_clstr    | prtn_keys |  rows  | raw_gb | cmpr_gb | cmpr_ratio | rows_min | rows_avg | rows_max | skw  | skw_pct | dev_pct
** ---------+----------+-------------+------------+-------------+------+--------------+--------------------+-----------+--------+--------+---------+------------+----------+----------+----------+------+---------+---------
**  testdb  |   420484 | public      | test       | ybdadmin    |    1 | hash(a)      |                    | a         |      0 |   0.00 |    0.00 |       0.00 |          |          |          |      |         |
**  testdb  |   420740 | public      | shardstore | kick        |   31 | hash(inode)  |                    |           | 662920 |   0.09 |    0.06 |       1.00 |    42661 |    44194 |    45309 | 1115 |    0.03 |   0.004
**  testdb  |   514857 | public      | team       | yellowbrick |    8 | hash(teamid) | clstr (name, city) |           |      0 |   0.00 |    0.00 |       0.00 |          |          |          |      |         |
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** This procedure overloads table_skew_p(VARCHAR) which returns a different rowtype.
*/
DROP TABLE IF EXISTS table_skew2_t CASCADE
;

CREATE TABLE table_skew2_t
(
   db_name        VARCHAR(128)
 , table_id       BIGINT
 , schema_name    VARCHAR(128)
 , table_name     VARCHAR(128)
 , distribution   VARCHAR(256)
 , row_avg        BIGINT
 , row_max        BIGINT
 , row_tot        BIGINT
 , row_skw        BIGINT
 , "row_skw%"     NUMERIC(19,1)
 , gb_avg         NUMERIC(19,3)
 , gb_max         NUMERIC(19,3)
 , gb_tot         NUMERIC(19,3)
 , gb_skw         NUMERIC(19,3)
 , "gb_skw%"      NUMERIC(19,1)
 , fullest_worker  VARCHAR(10000)
)
;

CREATE PROCEDURE table_skew_p(  _db_ilike       VARCHAR DEFAULT '%'
                              , _schema_ilike   VARCHAR DEFAULT '%'
                              , _table_ilike    VARCHAR DEFAULT '%'
                              , _yb_util_filter VARCHAR DEFAULT 'TRUE'
                             )
   RETURNS SETOF table_skew2_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT := '';
   _ret_rec   table_skew2_t%ROWTYPE;

   _fn_name   VARCHAR(256) := 'table_skew_p';
   _prev_tags VARCHAR(256) := CURRENT_SETTING('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;

BEGIN

   EXECUTE 'SET ybd_query_tags TO ''' || _tags || '''';
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);   

   _sql := $sql$WITH tbl_strg_sq AS
   (
   /* sys.table_storage is by table and worker */
   SELECT
      table_id::BIGINT                                           AS table_id
    , ROUND( AVG( rows_columnstore ), 0 )::BIGINT                AS rows_avg
    , ROUND( MIN( rows_columnstore ), 0 )::BIGINT                AS rows_min
    , ROUND( MAX( rows_columnstore ), 0 )::BIGINT                AS rows_max
    , ROUND( SUM( rows_columnstore ), 0 )::BIGINT                AS rows_sum

    , ROUND( AVG( compressed_bytes ), 0 )::BIGINT                AS cmpr_byt_avg
    , ROUND( MIN( compressed_bytes ), 0 )::BIGINT                AS cmpr_byt_min
    , ROUND( MAX( compressed_bytes ), 0 )::BIGINT                AS cmpr_byt_max
    , ROUND( SUM( compressed_bytes ), 0 )::BIGINT                AS cmpr_byt_sum

    , COUNT(*)                                                   AS blades
    , ROUND( SUM( uncompressed_bytes ), 0 )::BIGINT              AS uncmpr_byt_sum
   FROM
      sys.table_storage
   GROUP BY
      table_id
   LIMIT 1000000000 /* order by to force this to be evaluated before the joins */
   )
 , tbl_strg_mx AS    
   (
      SELECT
         table_id                               AS table_id
       , worker_id                              AS worker_id
       , '...' || RIGHT(worker_id::VARCHAR, 12) AS worker_id_str       
       , rows_columnstore                       AS rows
       , compressed_bytes                       AS compressed_bytes
      FROM
         sys.table_storage
   )
 , tbl_strg AS    
   (  SELECT
         tss.table_id
       , tss.rows_avg
       , tss.rows_min
       , tss.rows_max
       , tss.rows_sum
       , tss.cmpr_byt_avg
       , tss.cmpr_byt_min
       , tss.cmpr_byt_max
       , tss.cmpr_byt_sum
       , tss.blades
       , tss.uncmpr_byt_sum
       , string_agg( tsm.worker_id_str, ',' ) AS fullest_worker
      FROM tbl_strg_sq    AS tss
         JOIN tbl_strg_mx AS tsm 
            ON tss.table_id = tsm.table_id AND tss.cmpr_byt_max = tsm.compressed_bytes AND tss.rows_max = tsm.rows
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
   )
 , tbl_info AS
   ( SELECT
      d.name::VARCHAR( 128 )                                     AS db_name
    , t.table_id::BIGINT                                         AS table_id
    , s.name::VARCHAR( 128 )                                     AS schema_name
    , t.name::VARCHAR( 128 )                                     AS table_name
    , TRIM( u.usename::VARCHAR( 128 ) )                          AS table_owner
    , CASE
         WHEN t.distribution <> 'hash' THEN UPPER( t.distribution )
         ELSE '(' || t.distribution_key || ')'
      END::VARCHAR( 256 )                                        AS distribution
    , CASE
         WHEN t.sort_key     IS NOT NULL AND t.sort_key     != '' THEN '(' || t.sort_key || ')'
         WHEN t.cluster_keys IS NOT NULL AND t.cluster_keys != '' THEN '(' || t.cluster_keys || ')'
         ELSE NULL::VARCHAR
      END::VARCHAR( 1024 )                                       AS sort_or_clstr
    , t.partition_keys::VARCHAR( 1024 )                          AS prtn_keys
   FROM
      sys.table                                                  AS t
   FULL JOIN sys.schema                                          AS s ON t.database_id = s.database_id AND t.schema_id = s.schema_id
   INNER JOIN sys.database                                       AS d ON t.database_id = d.database_id
   INNER JOIN pg_user                                            AS u ON t.owner_id    = u.usesysid
   WHERE  d.name ILIKE $sql$ || quote_literal( _db_ilike     ) || $sql$
      AND s.name ILIKE $sql$ || quote_literal( _schema_ilike ) || $sql$
      AND t.name ILIKE $sql$ || quote_literal( _table_ilike  ) || $sql$
   )

   SELECT
      ti.db_name                                                    AS db_name
    , ti.table_id                                                   AS table_id
    , ti.schema_name                                                AS schema_name
    , ti.table_name                                                 AS table_name
    , ti.distribution                                               AS distribution
 -- , ti.sort_or_clstr                                              AS sort_or_clstr
 -- , ti.prtn_keys                                                  AS prtn_keys
    , ts.rows_avg                                                   AS row_avg
 -- , ts.rows_min                                                   AS row_mn
    , ts.rows_max                                                   AS row_max
    , ts.rows_sum                                                   AS row_tot
    ,( ts.rows_max - ts.rows_avg )::BIGINT                          AS row_skw
    ,( CASE
         WHEN ts.rows_avg = 0 AND ts.rows_max = 0 THEN NULL::NUMERIC( 19, 4 )
         WHEN ts.rows_avg = 0                     THEN ts.rows_max::NUMERIC( 19, 4 )
         ELSE(( ts.rows_max - ts.rows_avg ) / ts.rows_avg::NUMERIC( 19, 4 ) )
      END * 100 )::NUMERIC( 19, 1 )                                 AS "row_skw%"
    , ROUND( ts.cmpr_byt_avg /( 1024.0 ^3 ), 2 )::NUMERIC( 19, 3 )  AS gb_avg
 -- , ts.cmpr_byt_min                                               AS gb_min
    , ROUND( ts.cmpr_byt_max /( 1024.0 ^3 ), 2 )::NUMERIC( 19, 3 )  AS gb_max
    , ROUND( ts.cmpr_byt_sum /( 1024.0 ^3 ), 2 )::NUMERIC( 19, 3 )  AS gb_tot
    , ROUND( (ts.cmpr_byt_max - cmpr_byt_avg )/( 1024.0 ^3 ), 2 )::NUMERIC( 19, 3 )                          
                                                                    AS gb_skw
    ,( CASE
         WHEN ts.cmpr_byt_avg = 0                 THEN NULL::NUMERIC( 19, 6 )
         ELSE(( ts.cmpr_byt_max - ts.cmpr_byt_avg ) / ts.cmpr_byt_avg::NUMERIC( 19, 6 ) )
      END * 100 )::NUMERIC( 19, 1 )                                 AS "gb_skw%"
  --, ts.blades                                                     AS blades
    , ts.fullest_worker::VARCHAR(10000)                              AS fullest_worker
   FROM
      tbl_info   AS ti
   JOIN tbl_strg AS ts USING( table_id )
   WHERE $sql$ || _yb_util_filter || $sql$
   ORDER BY db_name, schema_name, table_name
   $sql$;

   --RAISE INFO '_sql=%', _sql;
   RETURN QUERY EXECUTE _sql ;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$proc$
;

COMMENT ON FUNCTION table_skew_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS
$cmnt$Description:
Row and storage skew summary for user tables by database, schema, and table with
worker(s).

Examples:
  SELECT * FROM table_skew_p( );
  SELECT * FROM table_skew_p( 'my_db', 's%');
  SELECT * FROM table_skew_p( '%', '%qtr%' ,'%fact%') 
     WHERE "row_skw%" > 10 AND row_avg > 100000 AND gb_skw > 1
     ORDER BY "row_skw%" DESC;

Arguments:
. _db_ilike       - (optl) An ILIKE pattern for the schema name. i.e. '%fin%'.
                    The default is '%'
. _schema_ilike   - (optl) An ILIKE pattern for the schema name. i.e. '%qtr%'.
                    The default is '%'
. _table_ilike    - (optl) An ILIKE pattern for the table name.  i.e. 'fact%'.
                    The default is '%'
. _yb_util_filter - (intrnl) for YbEasyCli use.

Note: 
. Tables that have no backend storage (i.e. tables created but not INSERTed into
  and tables that have been truncated are excluded from the query.

Version:
. 2023.06.01 - Yellowbrick Technical Support
$cmnt$
;



