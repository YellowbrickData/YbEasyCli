/* query_steps_p.sql
**
** Return the  current vs plan state of currently executing back-end statements
** by plan node.
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
** . 2020.04.25 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
** Example result:
**
**  query_id | wrkrs  | rows | rows_plan | mem_mb | mem_plan_mb | read_mb | write_mb | net_mb | net_cnt | run_sec | skew_pct |       node_plan
** ----------+--------+------+-----------+--------+-------------+---------+----------+--------+---------+---------+----------+-------------------------
**  14163643 | all    |   43 |           |     64 |             |         |          |        |         |     0.0 |     0.00 | SELECT                  
**           |        |      |           |        |             |         |          |        |         |         |          | (se.season_name, ma.matc
**           |        |      |           |        |             |         |          |        |         |         |          | distribute (ma.seasonid)
**  14163643 | all    |   43 |           |      8 |             |         |          |        |         |     0.0 |     0.00 | EXPRESSION              
**           |        |      |           |        |             |         |          |        |         |         |          | (se.season_name, ma.matc
*/

DROP TABLE IF EXISTS query_steps_t CASCADE ;
CREATE TABLE query_steps_t
   (
      query_id     BIGINT
    , wrkrs        VARCHAR (10)
    , rows         BIGINT
    , rows_plan    BIGINT 
    , mem_mb       BIGINT
    , mem_plan_mb  BIGINT
    , read_mb      BIGINT 
    , write_mb     BIGINT
    , net_mb       BIGINT
    , net_cnt      BIGINT
    , run_sec      NUMERIC (9, 1)
    , skew_pct     NUMERIC (9, 2)
    , node_plan    VARCHAR (16000)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE query_steps_p( _query_id_in BIGINT DEFAULT NULL ) 
   RETURNS SETOF query_steps_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _pred      TEXT := '';
   _sql       TEXT := '';
   
   _fn_name   VARCHAR(256) := 'query_steps_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
  
BEGIN

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   


   IF ( _query_id_in IS NOT NULL ) THEN
      _pred := 'WHERE q.query_id IN ( ' || _query_id_in || ' ) ';
   END IF;
   
   _sql := 'SELECT
      q.query_id                                       AS query_id
    , e.workers::VARCHAR(10)                           AS wrkrs
    , qa.rows_actual                                   AS rows_actl
    , qa.rows_planned                                  AS rows_plan    
    , CEIL (qa.memory_actual_bytes / 1024.0^2)::BIGINT AS mem_actl_mb
    , CEIL (qa.memory_planned_bytes/ 1024.0^2)::BIGINT AS mem_plan_mb    
    , CEIL (qa.io_read_bytes       / 1024.0^2)::BIGINT AS io_read_mb
    , CEIL (qa.io_write_bytes      / 1024.0^2)::BIGINT AS io_write_mb
    , CEIL (qa.io_network_bytes    / 1024.0^2)::BIGINT AS io_net_mb
    , qa.io_network_count                              AS io_net_cnt
    , ROUND (qa.runtime_ms / 1000.0, 2)::NUMERIC( 9,1) AS run_sec
    , (qa.skew::NUMERIC(16,6) * 100)::NUMERIC( 9,2)    AS skew_pct
    , e.query_plan::VARCHAR(16000)                     AS node_plan
   /*
    , e.type                                       AS step_type
    , e.index                                      AS index
    , e.node_id                                    AS node_id
    , v.level                                      AS level
    , qa.detail                                    AS detail
   */
   FROM
      sys.query                   q
      JOIN sys.query_explain      e ON q.plan_id = e.plan_id
      JOIN sys.vt_query_plan_node v ON e.plan_id = v.plan_id AND e.node_id = v.node_id
      JOIN (  SELECT
               query_id AS query_id
             , node_id
             , MAX (rows_planned)         AS rows_planned
             , SUM (rows_actual)          AS rows_actual
             , MAX (memory_planned_bytes) AS memory_planned_bytes
             , SUM (memory_actual_bytes)  AS memory_actual_bytes
             , SUM (io_read_bytes)        AS io_read_bytes
             , SUM (io_write_bytes)       AS io_write_bytes
             , SUM (io_network_bytes)     AS io_network_bytes
             , SUM (io_network_count)     AS io_network_count
             , MAX (runtime_ms)           AS runtime_ms
             , MAX (skew)                 AS skew
             , MAX (detail)               AS detail
            FROM
               sys.query_analyze
            GROUP BY
               query_id, node_id) qa ON q.query_id = qa.query_id AND e.node_id = qa.node_id
   ' || _pred || '
   ORDER BY
      q.query_id, e.index
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


COMMENT ON FUNCTION query_steps_p( BIGINT ) IS 
'Description:
Currently executing statements actual vs plan metrics by plan node.

Output is similar to EXPLAIN ANALYZE but for in-flight statements.
 
Examples:
  SELECT * FROM query_steps_p();
  SELECT * FROM query_steps_p( 1234 );

Arguments:
. _query_id_in - (Optional) Single query_id as a bigint.
                 Default is all currently executing back-end statements.
  
Note:
. For completed statements, use log_query_p().  
 
Revision:
. 2021.12.09 - Yellowbrick Technical Support
'
;