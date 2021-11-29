/* ****************************************************************************
** wlm_state_p()
**
** Current active WLM profile details.
**
** Usage:
**   See COMMENT ON FUNCTION text further below.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2021.04.22 - Yellowbrick Technical Support
** . 2021.11.21 - Integrated with YbEasyCli 
*/

/* ****************************************************************************
**  Example results:
**

** ...
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm_state_t CASCADE
;

CREATE TABLE wlm_state_t
   (
      pool_id    VARCHAR(128)
    , req_mb     VARCHAR(16)
    , slots      VARCHAR(32)
    , running    BIGINT
    , queued     BIGINT
    , planning   BIGINT
    , preparing  BIGINT
    , cancelling BIGINT
    , blocked    BIGINT
    , spilling   BIGINT
    , max_mins   DECIMAL(9,1)
   )
;

DROP PROCEDURE IF EXISTS wlm_state_p();

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE wlm_state_p()
   RETURNS SETOF wlm_state_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql TEXT := '';
   _ret_rec wlm_state_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'wlm_state_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   -- SET TRANSACTION       READ ONLY;
      
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   _sql := 'WITH active_profile AS
   ( SELECT
      profile                                                          AS profile
    , trim (name)                                                      AS name
    , max_concurrency || ''/'' || NVL (min_concurrency, max_concurrency) AS slots
    , requested_memory::VARCHAR(16)                                    AS req_mb
   FROM
      sys.wlm_resource_pool
   WHERE
      (  profile =
         (  SELECT name
            FROM   sys.wlm_active_profile
            WHERE  active = ''t''
         )
         OR profile IS NULL
      )
      AND activated =
      (  SELECT    MAX (activated)
         FROM      sys.wlm_resource_pool
      )
   )
  ,query AS
  (SELECT
     pool_id                                                                  AS pool_id
   , state                                                                    AS state
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                           AS submit_time
   , ROUND( io_spill_space_bytes / 1024.0^2, 2 )::DECIMAL(19,0)               AS spill_mb   
   FROM
     sys.query
   )
   
   SELECT 
     q.pool_id                                                     AS pool_id
   , p.req_mb                                                      AS req_mb
   , p.slots::VARCHAR(32)                                          AS slots
   , SUM( CASE WHEN q.state = ''running''      THEN 1 ELSE 0 END ) AS running
   , SUM( CASE WHEN q.state = ''queued''       THEN 1 ELSE 0 END ) AS queued
   , SUM( CASE WHEN q.state = ''planning''     THEN 1 ELSE 0 END ) AS planning
   , SUM( CASE WHEN q.state = ''preparing''    THEN 1 ELSE 0 END ) AS preparing
   , SUM( CASE WHEN q.state = ''cancelling''   THEN 1 ELSE 0 END ) AS cancelling
   , SUM( CASE WHEN q.state = ''blocked''      THEN 1 ELSE 0 END ) AS blocked
   , SUM( CASE WHEN NVL( q.spill_mb, 0 ) > 0 THEN 1 ELSE 0 END )   AS spilling
   , ROUND((MAX( ABS(extract( epoch FROM CURRENT_TIMESTAMP - q.submit_time ))) / 60.0)::DECIMAL(10,2),1 )::DECIMAL(9,1) 
                                                                   AS max_mins
   FROM active_profile p
      JOIN query       q ON p.name = q.pool_id
   GROUP BY 1, 2, 3
   ORDER BY 1, 2, 3
   ';

   RETURN QUERY EXECUTE _sql; 

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

END;   
$proc$ 
;

-- ALTER FUNCTION wlm_state_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION wlm_state_p() IS 
'Description:
Returns current active WLM profile state metrics by pool.
  
Examples:
  SELECT * FROM wlm_state_p();  
  
Arguments: 
.  None

Notes:
. Changes in the current profile are not reflected until saved/activated.

Revision:
. 2021.04.22 - Yellowbrick Technical Support
. 2021.11.21 - Integrated with YbEasyCli
'
;