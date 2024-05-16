/* log_query_explain_p.sql
**
** Execution plan for a completed query.
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
** . 2024.03.09 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Example result:
**
**  node_id |         type         | workers  |                           explain
** ---------+----------------------+----------+-------------------------------------------------------------
**        0 | SEQUENCE             | ALL      | SEQUENCE                                                   +
**          |                      |          | distribute none
**        1 | SELECT               | ALL      | SELECT                                                     +
**          |                      |          | (const.one)                                                +
**          |                      |          | distribute single
**        2 | SCAN VIRTUAL         | ALL      | SCAN VIRTUAL                                               +
**          |                      |          | (const.one)                                                +
**          |                      |          | distribute single
**          | -------------------- | -------- | ---------------------------------------------------------
**          | select               |          | ** plan_id ='+kfMlJCY7D5T5Sl7vrAD937U0pVGExijk69iO284o6A=' +
**          |                      |          | ** query_id=5348804401,  db_name ='feddb',  pool_id='large'+
**          |                      |          | ** rows    =         1,  state   ='done',  rstrt=0         +
**          |                      |          | ** prep_sec=      0.01,  exe_sec =0.01,  run_sec=0.01      +
**          |                      |          | ** read_mb =      0.00,  spill_mb=0.00                     +
**          |                      |          | **                                                         +
**          |                      |          |
**          | SQL                  |          | select * from sys.const
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_explain_t CASCADE;
CREATE TABLE         log_query_explain_t
(
      node_id  INT4           
    , type     VARCHAR(256)      
    , workers  VARCHAR(256) 
    , explain  VARCHAR(60000) 
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_explain_p( 
      _query_id_in     BIGINT 
      _max_query_chars INTEGER DEFAULT 64
    , _show_sql        INTEGER DEFAULT 0                                                
   ) 
   RETURNS SETOF log_query_explain_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _pred    TEXT := 'AND query_id IN ( ' || _query_id_in || ') ' ;
   _sql     TEXT := '';
   
   _fn_name   VARCHAR(256) := 'log_query_explain_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _new_tags  VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN
 
   -- Append sysviews proc to query tags
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _new_tags );     

   _sql := 'WITH query_info AS
   ( SELECT
      query_id                                                             AS query_id
    , database_name                                                        AS db_name
    , pool_id                                                              AS pool_id    
    , type::VARCHAR( 255 )                                                 AS type
    , state || DECODE( error_code, ''00000'', '''', '' '' || error_code)   AS state
    , num_restart                                                          AS rstrt  
    , ROUND( (parse_ms + plan_ms + assemble_ms + compile_ms) / 1000.0, 2 )::DECIMAL(19, 2) 
                                                                           AS prep_sec
    , ROUND(  run_ms                      / 1000.0   , 2 )::DECIMAL(19, 2) AS run_sec
    , ROUND( (run_ms - wait_run_cpu_ms )  / 1000.0   , 2 )::DECIMAL(19, 2) AS exe_sec
    , ROUND( io_read_bytes                / 1024.00^2, 2 )::DECIMAL(19, 2) AS read_mb    
    , ROUND( io_network_bytes             / 1024.00^2, 2 )::DECIMAL(19, 2) AS net_mb
    , ROUND( io_write_bytes               / 1024.00^2, 2 )::DECIMAL(19, 2) AS write_mb
    , ROUND( io_spill_write_bytes         / 1024.00^2, 2 )::DECIMAL(19, 2) AS spill_mb
    , GREATEST( rows_inserted ,rows_deleted,  rows_returned )              AS rows
    , plan_id                                                              AS plan_id
    , query_text                                                           AS query_text
   FROM sys.log_query
   WHERE type NOT IN( ''analyze'', ''copy'', ''deallocate'', ''describe'', ''flush''
                    , ''maintenance'', ''prepare'', ''session'', ''show'', ''unknown'' )
      ' || _pred || ' 
   )
   , query_plan_node AS
   (  SELECT
         qry.query_id                                                  AS query_id
       , qpn.plan_id                                                   AS plan_id
       , qpn.node_id                                                   AS node_id
       , qpn.index                                                     AS index
       , qpn.indent                                                    AS indent
       , qpn.type                                                      AS type
       , (CASE WHEN qpn.single_worker = TRUE THEN ''single'' ELSE ''ALL'' END
         )::VARCHAR( 16 )                                              AS workers
       , qpn.explain
        || NVL( CHR( 10 ) || rpad( '' '', qpn.indent ) ||                      qpn.output_columns   , '''' ) 
        || NVL( CHR( 10 ) || rpad( '' '', qpn.indent ) || ''distribute ''   || qpn.distribution     , '''' ) 
        || NVL( CHR( 10 ) || rpad( '' '', qpn.indent ) || ''partition by '' || qpn.partition_columns, '''' )
                                                                       AS explain_text
      FROM sys.yb_query_plan_node AS qpn
      JOIN query_info             AS qry ON qpn.plan_id = qry.plan_id
                 
      UNION ALL
      SELECT
         query_id                                                      AS query_id
       , plan_id                                                       AS plan_id
       , NULL::INT4                                                    AS node_id
       , 10000::INT4                                                   AS index
       , 0::INT                                                        AS indent
       , ''--------------------'' AS type
       , ''--------''                                                  AS workers
       , ''---------------------------------------------------------'' AS explain_text
      FROM query_info AS qry
      
      UNION ALL
      SELECT
         query_id                                                      AS query_id
       , plan_id                                                       AS plan_id
       , NULL::INT4                                                    AS node_id
       , 11000::INT4                                                   AS index
       , 0::INT                                                        AS indent
       , type                                                          AS type
       , NULL::VARCHAR( 16 )                                           AS workers
       ,     ''/* plan_id =''  || quote_literal(qry.plan_id) 
       || e''\n** query_id=''  || LPAD(qry.query_id, 10,'' '') || '',  db_name =''  || quote_literal(qry.db_name) || '',  pool_id='' || quote_literal(qry.pool_id)
       || e''\n** rows    =''  || LPAD(qry.rows    , 10,'' '') || '',  state   =''  || quote_literal(qry.state)   || '',  rstrt=''   || qry.rstrt
       || e''\n** prep_sec=''  || LPAD(qry.prep_sec, 10,'' '') || '',  exe_sec =''  || qry.exe_sec                || '',  run_sec='' || qry.run_sec
       || e''\n** read_mb =''  || LPAD(qry.read_mb,  10,'' '') || '',  spill_mb=''  || qry.spill_mb
       || e''\n*/      \n''
                                                                       AS explain_text
      FROM query_info AS qry
      
      UNION ALL
      SELECT
         query_id                                                      AS query_id
       , plan_id                                                       AS plan_id
       , NULL::INT4                                                    AS node_id
       , 12000::INT4                                                   AS index
       , 0::INT                                                        AS indent
       , ''SQL''                                                       AS type
       , NULL::VARCHAR( 16 )                                           AS workers
       , SUBSTR( qry.query_text, 1, ' || _max_query_chars || ')        AS explain_text
      FROM query_info AS qry
   )
    
   SELECT
      node_id::INT4                  AS node_id
    , type::VARCHAR(256)             AS type
    , workers::VARCHAR(  256)        AS workers
    , explain_text::VARCHAR(60000)   AS node_plan 
   FROM query_plan_node 
   ORDER BY index ASC
   ';

   IF ( _show_sql > 0 ) THEN RAISE INFO '_sql = %', _sql; END IF;  
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags ); 
     
END;
$proc$
;


COMMENT ON FUNCTION log_query_explain_p( BIGINT, INTEGER, INTEGER ) IS 
$cmnt$Description:
Execution plan for a completed query.

Examples:
  SELECT * FROM log_query_explain_p( 12345 );
  
  SELECT * FROM log_query_explain_p( 12345, 128 );
    
  SELECT node, node_plan 
  FROM ( SELECT * FROM log_query_explain_p( 12345, _show_sql:='t' ) ) sq;
  
Arguments:
. _query_id_in     BIGINT  (reqd) - a single query_id.
. _max_query_chars INTEGER (optl) - max number of charaters of query_text to display
                                    DEFAULT 64
. _show_sql        INTEGER (optl) - Show the executed SQL.
                                    DEFAULT: 0  

Version:
. 2024.03.20 - Yellowbrick Technical Support 
$cmnt$
;
