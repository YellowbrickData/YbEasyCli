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
** . 2022.08.19 - NVL (detail). 
** . 2022.08.18 - Fixed join problem, added additional columns, added custom option. 
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.08 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Example result:
**
sysviews=# SELECT * FROM log_query_steps_p( 3622477555, 'f' );
  query_id  | n | step | node | workers |  rows   | rows_plan | rows_mb | rows_plan_mb | mem_mb | mem_plan_mb | read_mb | write_mb | spill_mb | net_mb | net_cnt | run_sec | skew_pct |                                node_plan
------------+---+------+------+---------+---------+-----------+---------+--------------+--------+-------------+---------+----------+----------+--------+---------+---------+----------+--------------------------------------------------------------------------
 3622477555 | 0 |    0 |    0 | ALL     |       0 |   1670420 |       0 |            1 |      3 |           0 |       0 |        0 |        0 |      0 |       0 |    0.00 |     0.00 | SEQUENCE                                                                +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          | distribute none                                                         +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          |
 3622477555 | 0 |    1 |    1 | ALL     |       0 |   1670420 |       0 |            1 |     27 |          66 |       0 |        0 |        0 |      0 |     256 |    0.00 |     0.00 | SELECT                                                                  +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          | (yb_query_plan.plan_id)                                                 +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          | distribute (yb_query_plan.plan_id)                                      +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          |
 3622477555 | 0 |    2 |   11 | ALL     |       0 |   1670420 |       0 |            1 |      5 |           0 |       0 |        0 |        0 |      0 |       0 |    0.00 |     0.00 | ANTI LEFT HASH JOIN ON (_log_query_text.plan_id = yb_query_plan.plan_id)+
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          | (yb_query_plan.plan_id)                                                 +
            |   |      |      |         |         |           |         |              |        |             |         |          |          |        |         |         |          | distribute (yb_query_plan.plan_id)                                      +
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_steps_t CASCADE;
CREATE TABLE         log_query_steps_t
(
   query_id     BIGINT
 , n            INTEGER
 , step         BIGINT
 , node         INTEGER
 , workers      VARCHAR( 16 )
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
 , run_sec      NUMERIC( 9, 2 )
 , skew_pct     NUMERIC (19, 2)
 , node_plan    VARCHAR( 60000 )
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_steps_p( _query_id_in BIGINT 
                                             , _internal    BOOLEAN DEFAULT 'f'
                                             ) 
   RETURNS SETOF log_query_steps_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _pred    TEXT := 'AND query_id IN ( ' || _query_id_in || ') ' ;
   _sql     TEXT := '';
   
   _fn_name   VARCHAR(256) := 'log_query_steps_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _new_tags  VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN
 
   -- Append sysviews proc to query tags
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _new_tags );     

   _sql := 'WITH qry AS
   ( SELECT
      query_id                                                         AS query_id
    , num_restart                                                      AS rstrt      
    , type::VARCHAR( 255 )                                             AS type
    , database_name                                                    AS database_name
    , ROUND(  run_ms                     / 1000.0, 2 )::DECIMAL(19, 2) AS run_sec
    , ROUND( (run_ms - wait_run_cpu_ms ) / 1000.0, 2 )::DECIMAL(19, 2) AS exe_sec
    , ceil( io_spill_write_bytes / 1024.0^2 )                          AS spill_mb
    , state                                                            AS state
    , plan_id                                                          AS plan_id
   FROM
      sys.log_query
   WHERE type NOT IN( ''analyze'', ''copy'', ''deallocate'', ''describe'', ''flush'', ''maintenance'', ''prepare'', ''session'', ''show'', ''unknown'' )
      ' || _pred || ' 
   )
   , qea AS
   ( SELECT
      query_id                                                   AS query_id
    , node_id                                                    AS node_id
    , rows_planned                                               AS rows_plan
    , rows_actual                                                AS rows
    , ceil( row_size_actual_bytes  / 1024.00^2 ) ::bigint        AS rows_mb
    , ceil( row_size_planned_bytes / 1024.00^2 ) ::bigint        AS rows_plan_mb
    , ceil( memory_actual_bytes    / 1024.00^2 ) ::bigint        AS mem_mb
    , ceil( memory_planned_bytes   / 1024.00^2 ) ::bigint        AS mem_plan_mb
    , ceil( io_read_bytes          / 1024.00^2 ) ::bigint        AS read_mb
    , ceil( io_write_bytes         / 1024.00^2 ) ::bigint        AS write_mb
    , ceil( io_spill_write_bytes   / 1024.00^2 ) ::bigint        AS spill_mb
    , ceil( io_network_bytes       / 1024.00^2 ) ::bigint        AS net_mb
    , io_network_count::bigint                                   AS net_cnt
    , ROUND( runtime_ms       / 1000.0, 2 ) ::numeric( 9, 2 )    AS run_sec
    ,( skew::numeric( 16, 6 ) * 100 ) ::numeric( 19, 2 )         AS skew_pct
    , REPLACE( NVL(detail, '''') , '', '', e''\n'' )             AS detail
    , CASE WHEN ' || quote_literal( _internal ) ||  '::BOOLEAN
         THEN E''\n\tCustom:\n\t'' ||  REPLACE( NVL( custom::VARCHAR( 60000 ), ''''), ''},'', E''}\n\t,'' )
         ELSE '''' 
      END                                                        AS custom 
   FROM
      sys.yb_query_execution_analyze
   )
   , qpn AS
         (  SELECT
      plan_id                                                       AS plan_id
    , node_id                                                       AS node_id
    , index                                                         AS index
    , indent                                                        AS indent
    , type                                                          AS type
             , CASE
                  WHEN single_worker = true THEN ''single''
         ELSE ''ALL''
      END::varchar( 16 )                                            AS workers
             , explain
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || output_columns, '''' ) 
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || ''distribute ''   || distribution, '''' ) 
                  || COALESCE( CHR( 10 ) || rpad( '' '', indent ) || ''partition by '' || partition_columns, '''' )
               AS query_plan
            FROM
               sys.yb_query_plan_node
   )
    
   SELECT
      qry.query_id                                                  AS query_id
    , qry.rstrt                                                     AS n
    , qpn.index                                                     AS step   
    , qea.node_id                                                   AS node    
    , qpn.workers                                                   AS workers
    , qea.rows                                                      AS rows
    , qea.rows_plan                                                 AS rows_plan
    , qea.rows_mb                                                   AS rows_mb
    , qea.rows_plan_mb                                              AS rows_plan_mb
    , qea.mem_mb                                                    AS mem_mb
    , qea.mem_plan_mb                                               AS mem_plan_mb
    , qea.read_mb                                                   AS read_mb
    , qea.write_mb                                                  AS write_mb
    , qea.spill_mb                                                  AS spill_mb
    , qea.net_mb                                                    AS net_mb
    , qea.net_cnt                                                   AS net_cnt
    , qea.run_sec                                                   AS run_sec
    , qea.skew_pct                                                  AS skew_pct
    , (NVL(  CASE qpn.index
            WHEN -1 THEN ''''
            ||    ''/* query id: '' || qry.query_id  || '', db: ''       || qry.database_name
            || e''\n** type    : '' || qry.type      || '', state: ''    || qry.state || '', rstrt: '' || qry.rstrt
            || e''\n** run secs: '' || qry.run_sec   || '', exe secs: '' || qry.exe_sec
            || e''\n** spill mb: '' || qry.spill_mb
            || e''\n*/      \n''
            END   
         , ''''
         )
         || NVL( qpn.query_plan, '''' )
         || NVL( e''\n'' || rpad( '' '', qpn.indent ) || qea.detail, '''' )
         || qea.custom
      )::varchar( 60000 )                                           AS node_plan 
   FROM            qry
   LEFT OUTER JOIN qpn ON qry.plan_id  = qpn.plan_id
   LEFT OUTER JOIN qea ON qea.query_id = qry.query_id AND qea.node_id = qpn.node_id
   ORDER BY qry.query_id ASC, qry.rstrt ASC, step ASC
   ';

   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags ); 
     
END;
$proc$
;


COMMENT ON FUNCTION log_query_steps_p( BIGINT, BOOLEAN ) IS 
$cmnt$Description:
Completed statements actual vs plan metrics by plan node.

Examples:
  SELECT * FROM log_query_steps_p( 12345 );
  SELECT node, node_plan 
    FROM ( SELECT * FROM log_query_steps_p( 12345, 't' ) ) sq;
  
Arguments:
. _query_id_in - (required) a single query_id.

Version:
. 2022.08.18 - Yellowbrick Technical Support 
$cmnt$
;
