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
** . 2023.06.29 - first version.
*/

/* ****************************************************************************
**  Example results:
**
** table_id | database_name | schema_name | table_name | distribution | worker_count | worker_lid |    worker    | unit | data_total  | data_worker | data_skew | data_skew% | rows_total | rows_worker | rows_skew | rows_skew%
** ---------+---------------+-------------+------------+--------------+--------------+------------+--------------+------+-------------+-------------+-----------+------------+------------+-------------+-----------+------------
**  2630401 | test_database | public      | z_dropme   | inv_item_sk  |            8 |          5 | 38b8ebd0149b | B    | 24811405312 |  3126853632 |  33554432 |       1.08 | 1311525000 |   164834925 |    811975 |       0.50
**  1672464 | yellowbrick   | public      | z_dropme00 |              |            8 |          0 | 38b8ebd014cd | B    |    16777216 |     2097152 |         0 |       0.00 |          3 |           0 |         0 |       0.00
**  8815801 | yellowbrick   | public      | z_dropme01 | thread_id    |            8 |          0 | 38b8ebd014cd | B    |   150994944 |    25165824 |   6291456 |      33.33 |    8000008 |     1500004 |  500003.5 |      50.00
** (3 rows)
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** This procedure overloads table_skew_p(VARCHAR) which returns a different rowtype.
*/

DROP TABLE IF EXISTS table_skew_t CASCADE;

CREATE TABLE table_skew_t (
    table_id       BIGINT
  , database_name  VARCHAR(128)
  , schema_name    VARCHAR(128)
  , table_name     VARCHAR(128)
  , distribution   VARCHAR(256)
  , worker_count   INTEGER
  , worker_lid     INTEGER
  , worker         VARCHAR(32)
  , unit           CHAR(1)
  , data_total     BIGINT
  , data_worker    BIGINT
  , data_skew      DOUBLE PRECISION
  , "data_skew%"   NUMERIC(38,2)
  , rows_total     BIGINT
  , rows_worker    BIGINT
  , rows_skew      DOUBLE PRECISION
  , "rows_skew%"   NUMERIC(38,2)
);

-- Drop previous versions to avoid confusion
DROP PROCEDURE IF EXISTS table_skew_p(VARCHAR);
DROP PROCEDURE IF EXISTS table_skew_p(VARCHAR,VARCHAR,VARCHAR,VARCHAR);
DROP PROCEDURE IF EXISTS table_skew_p(BOOLEAN,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR);

CREATE OR REPLACE PROCEDURE table_skew_p (
    _detailed       BOOLEAN DEFAULT false
  , _unit           CHAR    DEFAULT 'B'
  , _db_ilike       VARCHAR DEFAULT '%'
  , _schema_ilike   VARCHAR DEFAULT '%'
  , _table_ilike    VARCHAR DEFAULT '%'
  , _yb_util_filter VARCHAR DEFAULT 'TRUE'
)
  RETURNS SETOF table_skew_t
  LANGUAGE 'plpgsql'
  VOLATILE
  CALLED ON NULL INPUT
  SECURITY DEFINER
AS
$proc$
DECLARE

  _sql       TEXT := '';
  _rec       record;
  _ret_rec   table_skew_t%ROWTYPE;
  _units     TEXT := 'BKMGTP';
  _power     SMALLINT;

  _fn_name   VARCHAR(128) := 'table_skew_p';
  _prev_tags VARCHAR(256) := CURRENT_SETTING('ybd_query_tags');
  _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;

BEGIN

  _power := position(_unit in _units) - 1;
  IF _power = -1 THEN
    RAISE 'Unsupported unit %', _unit;
  END IF;

  EXECUTE 'SET ybd_query_tags TO ''' || _tags || '''';
  PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

-- Step 1: materialize table storage info using database object filters.

  DROP TABLE IF EXISTS pg_temp.tsinfo;
  CREATE TEMP TABLE tsinfo (
      table_id         BIGINT
    , database_name    VARCHAR(128)
    , schema_name      VARCHAR(128)
    , table_name       VARCHAR(128)
    , distribution     VARCHAR(256)
    , worker_lid       INTEGER
    , worker           VARCHAR(32)
    , rows_columnstore BIGINT
    , compressed_bytes BIGINT
    , pwr              SMALLINT
  );
  _sql := $sql$SELECT
        table_id
      , d.name                              AS database_name
      , s.name                              AS schema_name
      , t.name                              AS table_name
      , Nvl(t.distribution_key, '<random>') AS distribution
      , w.logical_id                        AS worker_lid
      , split_part(ts.worker_id, '-', 5)    AS worker
      , ts.rows_columnstore
      , ts.compressed_bytes
    FROM sys.table_storage AS ts
      JOIN sys.TABLE       AS t
        JOIN sys.SCHEMA    AS s USING (schema_id, database_id)
        JOIN sys.DATABASE  AS d USING (database_id)
      USING (table_id)
      JOIN sys.worker      AS w USING (worker_id)
    WHERE ts.worker_id IS NOT NULL -- Only non-empty tables
      AND d.name ILIKE $sql$ || quote_literal( _db_ilike     ) || $sql$
      AND s.name ILIKE $sql$ || quote_literal( _schema_ilike ) || $sql$
      AND t.name ILIKE $sql$ || quote_literal( _table_ilike  );
  FOR _rec IN EXECUTE _sql
  LOOP
    INSERT INTO tsinfo (     table_id     , database_name,      schema_name,      table_name,      distribution,       worker,      worker_lid,      rows_columnstore,      compressed_bytes, pwr)
    VALUES             (_rec.table_id, _rec.database_name, _rec.schema_name, _rec.table_name, _rec.distribution , _rec.worker, _rec.worker_lid, _rec.rows_columnstore, _rec.compressed_bytes, _power);
  END LOOP;

-- Step 2: run the actual report query.

  _sql := $sql$WITH
    a AS (
      SELECT table_id, database_name, schema_name, table_name, distribution
          , count(DISTINCT worker) OVER (PARTITION BY table_id)::INTEGER                   AS worker_count
          , worker_lid
          , worker
      -- byte skew
          , sum(compressed_bytes) OVER (PARTITION BY table_id)                             AS data_total
          , compressed_bytes                                                               AS data_worker
          , median() WITHIN GROUP (ORDER BY compressed_bytes) OVER (PARTITION BY table_id) AS median_bytes
          , (compressed_bytes - median_bytes)                                              AS data_skew
          , round(compressed_bytes/(median_bytes*0.01)-100,2)                              AS data_skew_prc
      -- row skew
          , sum(rows_columnstore) OVER (PARTITION BY table_id)                             AS rows_total
          , rows_columnstore                                                               AS rows_worker
          , median() WITHIN GROUP (ORDER BY rows_columnstore) OVER (PARTITION BY table_id) AS rows_median
          , (rows_columnstore - rows_median)                                               AS rows_skew
          , IIF(rows_columnstore=0,0
              ,IIF(rows_median=0,100,round(rows_columnstore/(rows_median*0.01)-100,2)))    AS rows_skew_prc
          , pwr
      FROM tsinfo
    ),
    b AS (
      SELECT *, row_number() OVER (PARTITION BY table_id ORDER BY data_skew DESC, worker_lid) AS biggest_data_skew
      FROM a
    )
    SELECT table_id, database_name, schema_name, table_name, distribution
      , worker_count, worker_lid, worker, $sql$ || quote_literal(_unit) || $sql$::CHAR(1)
      , (data_total/1024^pwr)::BIGINT, (data_worker/1024^pwr)::BIGINT, data_skew/1024^pwr, data_skew_prc
      , rows_total, rows_worker, rows_skew, rows_skew_prc
    FROM b
    WHERE biggest_data_skew $sql$ || IIF(_detailed,'>0','=1') || $sql$
      AND $sql$ || _yb_util_filter || $sql$
    ORDER BY database_name, schema_name, table_name, worker_lid$sql$;

  --RAISE INFO '_sql=%', _sql;
  RETURN QUERY EXECUTE _sql ;

  -- Reset ybd_query_tags back to its previous value
  EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$proc$
;

COMMENT ON FUNCTION table_skew_p( BOOLEAN, CHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS
$cmnt$Description:
Row and storage skew summary for user tables by database, schema, and table with
worker(s). Only non-empty tables are included in the report.

Examples:
  -- Get report on all tables:
  SELECT * FROM table_skew_p();
  -- Get report on tables in 'my_db' database in all 'test%' schemas:
  SELECT * FROM table_skew_p(_db_ilike:='my_db', _schema_ilike:='test%');
  -- Get detailed report on my_db.my_schema.my_table table, displaying size in gigabytes:
  SELECT * FROM table_skew_p(_db_ilike:='my_db', _schema_ilike:='my_schema', _table_ilike:='my_table', _detailed:=true, _unit:='G');
  -- Get report on all '%fact%' tables in all '%qtr%' schemas in all databases, which have rows skew % > 10 and total size > 10 gigabytes
  SELECT * FROM table_skew_p(_db_ilike:='%'    , _schema_ilike:='%qtr%' , _table_ilike:='%fact%')
     WHERE "rows_skew%" > 10 AND data_total > 10
     ORDER BY "rows_skew%" DESC;

Arguments:
. _detailed       - show detailed report on tables, including all workers.
                    The default is false, meaning only one, the most data-skewed worker is reported per table.
. _unit           - reported data size unit, could be B (bytes), K (kilobytes), M (megabytes), G (gigabytes), T (terabytes) or P (petabytes)
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
. 2023.06.29 - Yellowbrick Technical Support
$cmnt$
;
