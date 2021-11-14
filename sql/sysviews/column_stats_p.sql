/* ****************************************************************************
** column_stats_p()
**
** Table column metdata including estimates from statistics.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.05 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | tbl_id  | table_schema | table_name | col | col_name | col_type | max_bytes | avg_width | rows | mb  
** -------------+---------+--------------+------------+-----+----------+----------+-----------+-----------+------+-----
**  yellowbrick | 2902298 | public       | ips        |   1 | my_id    | uuid     |        16 |         8 |    1 | 0.0 
**  yellowbrick | 2902298 | public       | ips        |   2 | i6       | ipv6     |        16 |         8 |    1 | 0.0 
** ...
** ... | dstnct_est | null_est |  dist_key   | sort_key | clstr_keys | partn_keys 
** ... +------------+----------+-------------+----------+------------+------------ 
** ... |          1 |        0 | hash(my_id) |          |            | 
** ... |          1 |        0 | hash(my_id) |          |            | 
** ... 
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS column_stats_t CASCADE
;

CREATE TABLE column_stats_t
   (
      db_name      VARCHAR (128)
    , tbl_id       BIGINT
    , table_schema VARCHAR (128)
    , table_name   VARCHAR (128)
    , col          SMALLINT
    , col_name     VARCHAR (128)
    , col_type     VARCHAR (32)
    , max_bytes    INTEGER
    , avg_width    INTEGER
    , rows         BIGINT
    , mb           NUMERIC(12,1)
    , dstnct_est   BIGINT
    , null_est     BIGINT
    , dist_key     VARCHAR (128)
    , sort_key     VARCHAR (128)
    , clstr_keys   VARCHAR (1024)
    , partn_keys   VARCHAR (1024)
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE column_stats_p(
    _db_name VARCHAR
    , _schema_ilike VARCHAR DEFAULT ''
    , _table_ilike VARCHAR DEFAULT ''
    , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF column_stats_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _addl_pred TEXT         := '';
   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'column_stats_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   /* Txn read_only to protect against potential SQL injection attack overwrites
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);
   
   
   IF ( TRIM(_schema_ilike) != '' AND TRIM(_table_ilike) != '' ) 
   THEN 
      _addl_pred := 'AND  n.nspname ILIKE ' 
      || CASE WHEN TRIM(_schema_ilike) = '' THEN quote_literal( '%' ) ELSE quote_literal( _schema_ilike ) END
      || ' AND c.relname ILIKE '       
      || CASE WHEN TRIM(_table_ilike)  = '' THEN quote_literal( '%' ) ELSE quote_literal( _table_ilike  ) END
      || ' ' || CHR(10);
   END IF;
   

   _sql := 'SELECT
      ' || quote_literal(_db_name) || '::VARCHAR(128)      AS db_name
    , c.oid::BIGINT                                        AS tbl_id
    , trim( n.nspname )::VARCHAR (128)                     AS table_schema
    , trim( c.relname )::VARCHAR (128)                     AS table_name
    , a.attnum                                             AS col
    , trim( a.attname )::VARCHAR (128)                     AS col_name
    , format_type( a.atttypid, a.atttypmod )::VARCHAR (32) AS col_type
    , CASE
         WHEN a.attlen = -1
         THEN (CASE
                  WHEN btrim(pt.typname::text) = ''numeric''
                  THEN (CASE WHEN a.atttypmod > 1245207 THEN 16  ELSE 8 END)
                  ELSE a.atttypmod
               END )
         ELSE a.attlen
      END                                                     AS max_bytes
    , s.stawidth                                              AS avg_width
    , ts.rows                                                 AS rows
    , ROUND((s.stawidth * ts.rows)/1024.0^2,1)::NUMERIC(12,1) AS mb 
    , CASE
         WHEN s.stadistinct > 0 THEN s.stadistinct::bigint
         ELSE                      ( s.stadistinct * - 1.0 * rows )::bigint
      END                                      AS dstnct_est
    ,( s.stanullfrac * rows )::bigint          AS null_est
    , CASE
         WHEN t.distribution <> ''hash'' THEN t.distribution
            ELSE t.distribution
               || ''(''
               || t.distribution_key
               || '')''
      END::VARCHAR (128)                       AS dist_key
    , trim( t.sort_key )::VARCHAR (128)        AS sort_key
    , trim( t.cluster_keys )::VARCHAR (1024)   AS clstr_keys
    , trim( t.partition_keys )::VARCHAR (1024) AS partn_keys
   /*, a.attnotnull                            AS not_null */ 

   FROM
      sys.table  t
   JOIN ' || _db_name || '.pg_catalog.pg_class c
      ON t.table_id = c.oid
   JOIN ' || _db_name || '.pg_catalog.pg_namespace n
      ON c.relnamespace = n.oid
   JOIN ' || _db_name || '.pg_catalog.pg_attribute a
      ON c.oid = a.attrelid
   JOIN ' || _db_name || '.pg_catalog.pg_type pt       
     ON a.atttypid = pt.oid   
   JOIN ' || _db_name || '.pg_catalog.pg_statistic s
      ON a.attrelid   = s.starelid
         AND a.attnum = s.staattnum
   JOIN
      (  SELECT table_id, SUM( rows_columnstore ) AS rows
         FROM      sys.table_storage
         GROUP BY  table_id
      )  ts
      ON t.table_id = ts.table_id 
   WHERE
      c.relkind      = ''r''::"char"
      AND t.table_id > 16384
      AND a.attnum   > 0
      ' || _addl_pred || '      
      AND ' || _yb_util_filter || '      
   ORDER BY
      2, 3, 4
   ';
 
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;

-- ALTER FUNCTION column_stats_p( VARCHAR, VARCHAR, VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION column_stats_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
Table column metadata including cardinality estimates from the db statistics.

Examples:
  SELECT * FROM column_stats_p( ''my_db'');
  SELECT * FROM column_stats_p( ''my_db'', ''s%'');
  SELECT * FROM column_stats_p( ''my_db'', ''%'' ,''%fact%'');  
  
Arguments:
. database_name - (required) This is case sensitive so will normally be all lower case.
. schema ilike  - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
. table  ilike  - (optional) An ILIKE pattern for the table name.  i.e. ''fact%''.

Version:
. 2020.06.15 - Yellowbrick Technical Support  
'
;