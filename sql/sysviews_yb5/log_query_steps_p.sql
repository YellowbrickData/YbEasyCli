/* log_query_steps_p.sql
**
** Actual runtime vs the plan statistics for a for completed query by plan node.
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
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.08 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Example result:
**
**  query_id | wrkrs  | rows | rows_plan | rows_mb | rows_plan_mb | mem_mb | mem_plan_mb | read_mb | write_mb | spill_mb | net_mb | net_cnt | run_sec | skew_pct |      node_plan
** ----------+--------+------+-----------+---------+--------------+--------+-------------+---------+----------+----------+--------+---------+---------+----------+---------------------------
**  13876962 | all    |    2 |        43 |    0.00 |         0.00 |   5.76 |       64.00 |       0 |        0 |        0 |      0 |     256 |     0.0 |   100.00 | ** query: 13876962, databa
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | ** run secs: 5.0,lock secs
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | ** state: 00000
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | **
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | SELECT
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | (se.season_name, ma.match_
**           |        |      |           |         |              |        |             |         |          |          |        |         |         |          | distribute (ma.seasonid)**
**  13876962 | all    |    2 |        43 |    0.00 |         0.00 |  18.50 |        8.00 |       0 |        0 |        0 |      0 |       0 |     5.0 |   100.00 | EXPRESSION calculate: (IF **
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_steps_t CASCADE;
CREATE TABLE         log_query_steps_t
(
   query_id     BIGINT
 , wrkrs        VARCHAR (16)
 , rows         BIGINT
 , rows_plan    BIGINT
 , rows_mb      BIGINT
 , rows_plan_mb BIGINT
 , mem_mb       BIGINT
 , mem_plan_mb  BIGINT
 , read_mb      BIGINT
 , write_mb     BIGINT
 , spill_mb     BIGINT    
 , net_mb       BIGINT
 , net_cnt      BIGINT
 , run_sec      NUMERIC (9, 1)
 , skew_pct     NUMERIC (19, 2)
 , node_plan    VARCHAR (16000)
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_steps_p( _query_id_in BIGINT ) 
   RETURNS SETOF log_query_steps_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _pred    TEXT := 'AND q.query_id IN ( ' || _query_id_in || ') ' ;
   _sql     TEXT := '';
   
   _fn_name   VARCHAR(256) := 'log_query_steps_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN
 
   --SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _sql := 'SELECT
      q.query_id                                                 AS query_id
    , e.workers::VARCHAR (16)                                    AS wrkrs
    , a.rows_actual                                              AS rows
    , a.rows_planned                                             AS rows_plan
    , CEIL( a.row_size_actual_bytes    / 1024.00^2 )::BIGINT     AS rows_mb
    , CEIL( a.row_size_planned_bytes   / 1024.00^2 )::BIGINT     AS rows_plan_mb
    , CEIL( a.memory_actual_bytes      / 1024.00^2 )::BIGINT     AS mem_mb
    , CEIL( a.memory_planned_bytes     / 1024.00^2 )::BIGINT     AS mem_plan_mb
    , CEIL( a.io_read_bytes            / 1024.00^2 )::BIGINT     AS read_mb
    , CEIL( a.io_write_bytes           / 1024.00^2 )::BIGINT     AS write_mb
    , CEIL( a.io_spill_write_bytes     / 1024.00^2 )::BIGINT     AS spill_mb
    , CEIL( a.io_network_bytes         / 1024.00^2 )::BIGINT     AS net_mb
    , a.io_network_count::BIGINT                                 AS net_cnt
    , ROUND( a.runtime_ms / 1000.0, 1 )::NUMERIC (9, 1)          AS run_sec
    , (a.skew::NUMERIC(16,6) * 100)::NUMERIC( 19, 2)             AS skew_pct
    , CASE e.index
         WHEN 0 THEN ''/* query: ''      || q.query_id
               || '', type: ''           || q.type
               || '', db: ''             || q.database_name
               || e''\n** run secs: ''   || ROUND( q.run_ms / 1000, 1 )
               || '', spill write mb: '' || TRUNC( q.io_spill_write_bytes / 1024.000^2, 2 )
               || e''\n** state: ''      || q.state
               || e'' \n*/\n''          
               || e.query_plan || COALESCE( e''\n''
               || rpad( '' '', e.indent ) ||a.detail, '''' )
         ELSE e.query_plan || COALESCE( e''\n''
               ||rpad( '' '', e.indent ) ||a.detail, '''' )
      END::VARCHAR (16000)                                       AS node_plan
   /*
    , e.type                                                     AS step_type
    , e.index                                                    AS index
    , a.node_id                                                  AS node
   */   
   FROM
      sys.log_query q
      LEFT OUTER JOIN
         (  SELECT
               plan_id
             , node_id
             , INDEX
             , indent
             , type
             , CASE
                  WHEN single_worker = true THEN ''single''
                  ELSE ''all''
               END AS workers
             , explain
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || output_columns, '''' ) 
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || ''distribute ''   || distribution, '''' ) 
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || ''partition by '' || partition_columns, '''' )
               AS query_plan
            FROM
               sys.yb_query_plan_node
         ) e ON e.plan_id = q.plan_id
       
      LEFT OUTER JOIN
         (  SELECT
               query_id
             , node_id
             , rows_planned
             , rows_actual
             , row_size_planned_bytes
             , row_size_actual_bytes
             , memory_planned_bytes
             , memory_actual_bytes
             , io_read_bytes
             , io_write_bytes
             , io_spill_write_bytes
             , io_network_bytes
             , io_network_count
             , runtime_ms
             , skew
             , REPLACE( detail, '', '', e''\n'' ) AS detail
            FROM
               sys.yb_query_execution_analyze
         ) a ON a.query_id = q.query_id AND a.node_id = e.node_id
    
   WHERE
      q.type NOT IN( ''analyze'', ''copy'', ''deallocate'', ''describe''
                  , ''flush''   , ''maintenance'', ''prepare'', ''session''
                  , ''show''    , ''unknown'' )
      AND q.query_text <> ''System Work''
     ' || _pred || '
   ORDER BY
      q.query_id ASC, e.index ASC
   ';

   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   
     
END;
$proc$
;


COMMENT ON FUNCTION log_query_steps_p( BIGINT ) IS 
'Description:
Completed statements actual vs plan metrics by plan node.

Examples:
  SELECT * FROM log_query_steps_p( 12345 );
  
Arguments:
. _query_id_in - (required) a single query_id.

Version:
. 2021.12.09 - Yellowbrick Technical Support 
'