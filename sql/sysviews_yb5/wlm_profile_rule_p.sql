/* ****************************************************************************
** wlm_profile_rule_p()
**
** Current active or named WLM detailed profile rules.
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
** . 2023.04.06 - Fix for return row size.
** . 2023.04.04 - Change ordering of comment lines in rule output begining.
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.05 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
** //------------------------------------------------
** //Rule Count: 001
** //Rule:       global_throttleConcurrentQueries
** //Order:      1
** //Type:       submit
** //Applies To: user
** //Enabled:    yes
** //Profile:    (global)
** 
** // Limit maximum number of concurrent queries.
** // Note that maximum number of user connections is not affected by this rule.
** // See Database Limits in the Appliance's documentation (max_connections and max_user_connections).
** //
** // Note: do not throttle on analyze hll, which is run as an implied query after CTAS and
** //       INSERT/SELECT query types.
** if (w.type !== 'analyze') {
**   wlm.throttle(500);
** }
** 
** //------------------------------------------------
** //Rule Count: 002
** //Rule:       global_throttleExternalTables
** //Order:      1
** //Type:       compile
** //Applies To: user
** //Enabled:    yes
** //Profile:    (global)
** 
** if (w.isExternalScan()) {
**     log.debug("Taking a external scan throttle");
**     wlm.throttle(5);
** } else if (w.isExternalWrite()) {
**     log.debug("Taking a external write throttle");
**     wlm.throttle(5);
** 
** }
**
**  ...
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm_profile_rule_t CASCADE
;

CREATE TABLE wlm_profile_rule_t
(
   rule VARCHAR (60000)
)
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE wlm_profile_rule_p( _profile_name VARCHAR DEFAULT '' )
RETURNS SETOF wlm_profile_rule_t 
   LANGUAGE 'plpgsql'
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql         TEXT         := '';
   _rule_clause VARCHAR      := 'wap.active IS TRUE';
   
   _fn_name     VARCHAR(256) := 'wlm_profile_rule_p';
   _prev_tags   VARCHAR(256) := current_setting('ybd_query_tags');
   _tags        VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';

   IF _profile_name <> '' THEN
      _rule_clause := 'war.profile_name IN (''' || _profile_name || ''')';
   END IF;

   _sql := REPLACE($str$
WITH
rule AS (
   SELECT
      DECODE(war.rule_type, 'submit', 10, 'assemble', 20, 'compile', 30, 'run', 40, 'runtime', 40, 'restart_for_error', 50, 'restart_for_user', 60, 'completion', 70
         , 80) AS rule_type_order
      , TO_CHAR(ROW_NUMBER() OVER(ORDER BY profile_name, rule_type_order, "order", superuser, rule_name), '000') AS rn
      ,             '//------------------------------------------------'
      || CHR(10) || '//Rule Count:'  || rn
      || CHR(10) || '//Rule:       ' || war.rule_name
      || CHR(10) || '//Order:      ' || war.order::VARCHAR
      || CHR(10) || '//Type:       ' || war.rule_type
      || CHR(10) || '//Applies To: ' || DECODE(superuser, TRUE, 'superuser', 'user')
      || CHR(10) || '//Enabled:    ' || DECODE(enabled, TRUE, 'yes', 'no')
      || CHR(10) || '//Profile:    ' || war.profile_name
      || CHR(10)
      || CHR(10) || war.expression AS rule_att
      , war.superuser
      , war."order", war.profile_name, war.rule_name
   FROM
      sys.wlm_active_rule AS war
      LEFT JOIN sys.wlm_active_profile AS wap
         ON (wap.name = war.profile_name)
   WHERE 
      {rule_clause}
      OR war.profile_name = '(global)'
)
SELECT
    rule_att::VARCHAR(60000)
FROM rule
ORDER BY rn
$str$, '{rule_clause}', _rule_clause);

   --RAISE INFO '_sql: %', _sql;
   
   RETURN QUERY EXECUTE _sql; 

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';  

END;   
$proc$ 
;


COMMENT ON FUNCTION wlm_profile_rule_p( _profile_name VARCHAR ) IS 
$str$Description:
Current active or named WLM detailed profile rules.
  
Examples:
  SELECT * FROM wlm_profile_rule_p(); 
  SELECT * FROM wlm_profile_rule_p( 'my_profile' );  

Arguments: 
. _profile_name VARCHAR (optl) - Choose the profile to report on.
                                 Default is the current active profile

Notes:
. Changes in the current profile are not reflected until saved/activated.

Version:
. 2021.12.09 - Yellowbrick Technical Support 
$str$
;