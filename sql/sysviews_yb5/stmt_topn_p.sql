/* ****************************************************************************
** stmt_topn_p.sql
**
** The top <n> (worst) performing queries across multiple columns.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2022.04.09 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**    top    |  query_id  |     submit_time     | pool_id | state  | code  |      username      |  app_name   | tags |  type   
** ------------+------------+---------------------+---------+--------+-------+--------------------+-------------+------+---------
**  run_sec    | 2610760103 | 2022-04-05 18:30:48 | large   | done   | 00000 | sys_ybd_replicator | replication |      | restore 
**  run_sec    | 2633225980 | 2022-04-08 09:26:02 | large   | done   | 00000 | sys_ybd_replicator | replication |      | restore 
**  run_sec    | 2622200868 | 2022-04-07 02:48:35 | large   | cancel | 57014 | yellowbrick        | ybsql       |      | select  
**  run_sec    | 2575707743 | 2022-04-01 10:08:00 | small   | done   | 00000 | eugene             | ybunload    |      | unload  
** ... 
** ...|    rows    | restart | plnr_sec | cmpl_sec | que_sec | exe_sec | io_wt_sec | run_sec | tot_sec | max_mb | spl_wrt_mb | query_text 
** ...+------------+---------+----------+----------+---------+---------+-----------+---------+---------+--------+------------+------------ 
** ...| 6658457600 |       0 |      0.0 |      0.0 |     0.0 |    17.2 |     148.1 |   165.4 |   165.4 |   1508 |          0 | YRESTORE W 
** ...| 3320003320 |       0 |      0.0 |      0.0 |     0.0 |     8.3 |      72.3 |    80.7 |    80.7 |   1580 |          0 | YRESTORE W 
** ...|   56215878 |       0 |      0.0 |      0.0 |     0.0 |     3.1 |      69.2 |    77.7 |  2050.5 |    896 |          0 | select * f 
** ...|          0 |       0 |      0.1 |      1.0 |     0.0 |     1.0 |      47.5 |    48.5 |    49.6 |    278 |          0 | YUNLOAD (s 
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS stmt_topn_t CASCADE
;

CREATE TABLE stmt_topn_t
(
   top                        VARCHAR( 128 )
 , query_id                   BIGINT NOT NULL
 , submit_time                TIMESTAMP     
 , pool_id                    VARCHAR( 128 )
 , state                      VARCHAR(  50 )  
 , code                       VARCHAR(   5 )
 , username                   VARCHAR( 128 )
 , app_name                   VARCHAR( 128 )
 , tags                       VARCHAR( 255 )
 , type                       VARCHAR( 128 )
 , rows                       BIGINT
 , restart                    INTEGER 
 , plnr_sec                   NUMERIC( 19, 1 ) 
 , cmpl_sec                   NUMERIC( 19, 1 ) 
 , que_sec                    NUMERIC( 19, 1 )
 , exe_sec                    NUMERIC( 19, 1 ) 
 , io_wt_sec                  NUMERIC( 19, 1 )  
 , run_sec                    NUMERIC( 19, 1 )
 , tot_sec                    NUMERIC( 19, 1 )
 , max_mb                     NUMERIC( 19, 0 )
 , spl_wrt_mb                 NUMERIC( 19, 0 ) 
 , query_text                 VARCHAR( 60000 )
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE stmt_topn_p(
      _limit          INTEGER DEFAULT 50
    , _col_names      VARCHAR DEFAULT 'exe_sec,run_sec,spl_wrt_mb'
    , _date_begin     DATE    DEFAULT CURRENT_DATE - 7
    , _date_end       DATE    DEFAULT CURRENT_DATE
    , _yb_util_filter VARCHAR DEFAULT 'TRUE'   
   )
   RETURNS SETOF stmt_topn_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _arr_delim         TEXT    := ',';
   _col_name          VARCHAR := ',';   
   _col_names_arr     VARCHAR[];
   _query_text_chars INTEGER  := 48;
   _sql              TEXT     := '';

   _fn_name   VARCHAR(256) := 'stmt_topn_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _new_tags  VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- Append sysviews:stmt_topn to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _new_tags );
   
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);     

   _col_names_arr := TRIM(string_to_array( _col_names , _arr_delim ));
	IF ( _col_names_arr = NULL ) OR ( LENGTH( trim( _col_names_arr ) ) = 0 ) 
   THEN 
      RAISE INFO '_col_names value is empty or null';   
   END IF;

   FOR i IN array_lower( _col_names_arr, 1 ) .. array_upper( _col_names_arr, 1 )
   LOOP
      _col_name := TRIM(_col_names_arr[i]);
      -- RAISE INFO '_col_name: %',  _col_name;    

      _sql := 'SELECT
      ' || quote_literal( _col_name ) || '::VARCHAR(128)                                         AS top  
      , query_id                                                                                 AS query_id
      , date_trunc( ''secs'', submit_time )::TIMESTAMP                                           AS submit_time
      , pool_id::VARCHAR( 128 )                                                                  AS pool_id
      , state::VARCHAR( 50 )                                                                     AS state
      , error_code::VARCHAR( 5 )                                                                 AS code   
      , username::VARCHAR( 128 )                                                                 AS username 
      , SPLIT_PART( application_name, '' '', 1 )::VARCHAR(128)                                   AS app_name
      , SPLIT_PART( tags            , '' '', 1 )::VARCHAR(255)                                   AS tags
      , type                                                                                     AS type
      , GREATEST( rows_deleted, rows_inserted, rows_returned )                                   AS rows
      , ROUND( num_restart / 1000.0, 2 )::INTEGER                                                AS restart 
      , ROUND( ( parse_ms   + wait_parse_ms + wait_lock_ms + plan_ms + wait_plan_ms + assemble_ms + wait_assemble_ms                     
               ) / 1000.0, 2 )::DECIMAL(19,1)                                                    AS plnr_sec
      , ROUND( (compile_ms + wait_compile_ms ) / 1000.0, 2 )::DECIMAL(19,1)                      AS cmpl_sec                                                                             
      , ROUND( acquire_resources_ms            / 1000.0, 2 )::DECIMAL(19,1)                      AS que_sec   
      , ROUND( ( run_ms -( wait_run_io_ms + wait_run_cpu_ms ) ) /( 1000.0), 1 )::DECIMAL(19,1)   AS exe_sec
      , ROUND( ( wait_run_io_ms ) /( 1000 ), 1 )::DECIMAL(19,1)                                  AS io_wt_sec     
      , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)                      AS run_sec
      , ROUND( total_ms                        / 1000.0, 2 )::DECIMAL(19,1)                      AS tot_sec
      , CEIL ( memory_bytes                    / 1024.0^2  )::DECIMAL(19,0)                      AS max_mb
      , CEIL ( io_spill_write_bytes            / 1024.0^2  )::DECIMAL(19,0)                      AS spl_wrt_mb   
      , TRANSLATE( SUBSTR( query_text, 1,' || _query_text_chars ||' ), e''\n\t\r'', ''  '' )::VARCHAR(60000) 
                                                                                                 AS query_text
      FROM
        sys.log_query
      WHERE submit_time::DATE >= ' || quote_literal( _date_begin ) || '
        AND submit_time::DATE <= ' || quote_literal( _date_end   ) || '
        AND pool_id IS NOT NULL
        AND ' || quote_ident( _col_name )  || ' IS NOT NULL
        AND ' ||_yb_util_filter || '
      ORDER BY ' || quote_ident( _col_name )  || ' DESC     
      LIMIT ' || _limit;
      
      --RAISE INFO '_sql: %', _sql;	
      RETURN QUERY EXECUTE _sql;
      
   END LOOP;   

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );
   
END;   
$proc$
;

   
COMMENT ON FUNCTION stmt_topn_p( INTEGER, VARCHAR, DATE, DATE, VARCHAR ) IS 
$cmnt$Description:
The top <n> (i.e. worst) performing statements across multiple columns.
  
Examples:
  SELECT * FROM stmt_topn_p( ) 
  SELECT * FROM stmt_topn_p( 10, 'run_sec, spl_wrt_mb' );
  SELECT * FROM stmt_topn_p( 10, 'run_sec', '2022-03-01', CURRENT_DATE  );     
  SELECT * FROM stmt_topn_p( 10, $$run_sec$$, $$2022-03-01$$, $$2022-03-31$$, $$type NOT LIKE '%load%'$$ );       
  
Arguments:
. _limit          - (optional) The number of rows to return for each "top n" type.
                               Default: 50
. _col_names      - (optional) Column(s) to do a "top n" of. 
                               Default: 'exe_sec,run_sec,spl_wrt_mb'
. _date_begin     - (optional) Statement minimum date. Default: CURRENT_DATE - 7.
. _date_end       - (optional) Statement maximum date. Default: CURRENT_DATE.
. _yb_util_filter - (internal) Used by YbEasyCli.

Version:
. 2022.04.09 - Yellowbrick Technical Support
$cmnt$
;