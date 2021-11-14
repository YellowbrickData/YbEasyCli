/* ****************************************************************************
** wlm_active_profile_p()
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
** . 2021.05.08 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**  profile |  name  | slots | max_queue | max_wait | max_rows | max_time | req_mem | max_spill_pct | sys |      activated
** ---------+--------+-------+-----------+----------+----------+----------+---------+---------------+-----+---------------------
**  default | admin  | 2/2   |      1000 |          |          |          | 4096    | 5.0           | t   | 2020-02-07 13:39:21
**  default | large  | 4/4   |      1000 |          |          |          |         | 55.0          | t   | 2020-02-07 13:39:21
** ...
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm_active_profile_t CASCADE
;

CREATE TABLE wlm_active_profile_t
   (
      profile       VARCHAR(128)
    , name          VARCHAR(128)
    , slots         VARCHAR(9)
    , max_queue     INTEGER
    , max_wait      INTEGER
    , max_rows      BIGINT
    , max_time      INTEGER
    , req_mem       VARCHAR(16)
    , max_spill_pct VARCHAR(16)
    , sys           BOOLEAN
    , activated     TIMESTAMP
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE wlm_active_profile_p()
   RETURNS SETOF wlm_active_profile_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql TEXT := '';
   
   _fn_name   VARCHAR(256) := 'wlm_active_profile_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   --SET TRANSACTION       READ ONLY;
      
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   _sql := 'SELECT
      profile::VARCHAR(128)                                              AS profile
    , name::VARCHAR(128)                                                 AS name
    , (max_concurrency::VARCHAR(4)|| ''/'' || NVL(min_concurrency, max_concurrency)::VARCHAR(4))
                                                                         AS slots
    , queue_size                                                         AS max_queue
    , maximum_wait_limit                                                 AS max_wait
    , maximum_row_limit                                                  AS max_rows
    , maximum_exec_time_limit                                            AS max_time
    , requested_memory::VARCHAR(16)                                      AS req_mem
    , max_spill_pct::VARCHAR(16)                                         AS max_spill_pct
    , system                                                             AS sys
    , date_trunc(''secs'', activated)::TIMESTAMP                         AS activated
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
   ORDER BY
      1 , 2
   ';

   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

END;   
$proc$ 
;

-- ALTER FUNCTION wlm_active_profile_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION wlm_active_profile_p() IS 
'Description:
Returns current active WLM profile configuration details by pool.
  
Examples:
  SELECT * FROM wlm_active_profile_p();  
  
Arguments: 
.  None

Notes:
. Changes in the current profile are not reflected until saved/activated.

Revision History:
. 2021.05.08 - Yellowbrick Technical Support 
'
;