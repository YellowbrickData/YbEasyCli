/* ****************************************************************************
** wlm_active_rule_p()
**
** Current active WLM profile rules.
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
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.05 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**  profile_name |             rule_name         | rule_type  |  order  | user_type |                expression
** --------------+-------------------------------+------------+---------+-----------+------------------------------------------
**  default      | default_logLongRunningQuery   | completion |       1 | user      | if (w.duration > 30000) {   log.error('Q
**  (global)     | global_throttleExternalTables | prepare    |       1 | user      | if (w.isExternalScan()) {     log.debug( ...
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm_active_rule_t CASCADE
;

CREATE TABLE wlm_active_rule_t
   (
      profile_name VARCHAR (256)
    , rule_name    VARCHAR (256)
    , rule_type    VARCHAR (16)
    , rule_order   INTEGER
    , user_type    VARCHAR (40)
    , expression   VARCHAR (60000)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE wlm_active_rule_p( _expr_chars INTEGER DEFAULT 32 )
RETURNS SETOF wlm_active_rule_t 
   LANGUAGE 'plpgsql'
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql TEXT := '';
   
   _fn_name   VARCHAR(256) := 'wlm_active_rule_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   _sql := 'SELECT 
      profile_name::VARCHAR(256)                                              AS profile_name
    , rule_name::VARCHAR(256)                                                 AS rule_name
    , rule_type::VARCHAR(16)                                                  AS rule_type
    , "order"                                                                 AS rule_order
    , CASE WHEN superuser = ''t'' THEN ''superuser'' ELSE ''user'' END::VARCHAR(40)
                                                                              AS user_type
    , TRANSLATE (SUBSTR (expression, 1, ' || _expr_chars || '), e''\n\t'', '' '')::VARCHAR(60000)   
                                                                              AS expression
   FROM sys.wlm_active_rule
   WHERE profile_name IN (  SELECT name
         FROM sys.wlm_active_profile
         WHERE active = ''t'') OR profile_name = ''(global)''
   ORDER BY "order", rule_type, profile_name
   ';

   --RAISE INFO '_sql: %', _sql;
   
   RETURN QUERY EXECUTE _sql; 

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

END;   
$proc$ 
;

-- ALTER FUNCTION wlm_active_rule_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION wlm_active_rule_p( INTEGER ) IS 
'Description:
Current active WLM profile rules.
  
Examples:
  SELECT * FROM wlm_active_rule_p(); 
  SELECT * FROM wlm_active_rule_p( 80 );  
  
Arguments: 
. _expr_chars - (optional) INTEGER - max number of chars of WLM JavaScript text
                 to display. Default: 32

Notes:
. Changes in the current profile are not reflected until saved/activated.

Version:
. 2021.05.08 - Yellowbrick Technical Support 
'
;