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
** . 2023.01.20 - Fix to COMMENT ON.
** . 2022.04.11 - Increase rule_type width to 24.
** . 2022.03.05 - Cosmetic updates.
** . 2021.12.09 - ybCliUtils inclusion.
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
    , rule_type    VARCHAR (24)
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

   -- Append sysviews:proc_name to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _tags );

   _sql := 'SELECT 
      profile_name::VARCHAR(256)                                              AS profile_name
    , rule_name::VARCHAR(256)                                                 AS rule_name
    , rule_type::VARCHAR(24)                                                  AS rule_type
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

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );


END;   
$proc$ 
;


COMMENT ON FUNCTION wlm_active_rule_p( INTEGER ) IS 
$cmnt$Description:
Current active WLM profile rules.

The expression text is truncated with newlines removed so it can be displayed
on a single line.  

Examples:
  SELECT * FROM wlm_active_rule_p(); 
  SELECT * FROM wlm_active_rule_p( 80 );  
  
Arguments: 
. _expr_chars - (optl) INTEGER - max number of chars of WLM JavaScript text
                 to display. Default: 32

Notes:
. Changes in the current profile are not reflected until saved/activated.

Version:
. 2023.01.20 - Yellowbrick Technical Support 
$cmnt$
;