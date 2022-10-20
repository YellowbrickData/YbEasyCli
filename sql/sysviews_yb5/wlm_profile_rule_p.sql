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
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.05 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**  //Rule Count: 001
**  //Enabled:    yes
**  //Applies To: user
**  //Type:       submit
**  //Order:      1
**  //Profile:    (global)
**  //Rule:       global_throttleConcurrentQueries
**  if (w.type !== 'analyze') {
**    wlm.throttle(500);
**  }
**
**  //Rule Count: 002
**  //Enabled:    yes
**  //Applies To: user
**  //Type:       compile
**  //Order:      10000
**  //Profile:    (global)
**  //Rule:       global_restartErrorPolicy
**   w.errorRecoverable = (w.numRestartError == 0);
**
**  //Rule Count: 003
**  //Enabled:    no
**  //Applies To: user
**  //Type:       compile
**  //Order:      10000
**  //Profile:    (global)
**  //Rule:       global_defaultRowLimit
**  var maxRows = 5000000;
**  if (w.type === 'select') {
**      w.maximumRowLimit = Math.min(w.maximumRowLimit || maxRows, maxRows)
**  }
**
**  ...
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm__rule_t CASCADE
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

   _sql TEXT := '';
   _rule_clause VARCHAR := 'wap.active IS TRUE';
   
   _fn_name   VARCHAR(256) := 'wlm_profile_rule_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   IF _profile_name <> '' THEN
      _rule_clause := 'war.profile_name IN (''' || _profile_name || ''')';
   END IF;

   _sql := REPLACE($str$
WITH
rule AS (
   SELECT
      '//Enabled:    ' || DECODE(enabled, TRUE, 'yes', 'no')
      || CHR(10) || '//Applies To: ' || DECODE(superuser, TRUE, 'superuser', 'user')
      || CHR(10) || '//Type:       ' || war.rule_type
      || CHR(10) || '//Order:      ' || war.order::VARCHAR
      || CHR(10) || '//Profile:    ' || war.profile_name
      || CHR(10) || '//Rule:       ' || war.rule_name
      || CHR(10) || war.expression AS rule_att
      , war.superuser
      , DECODE(war.rule_type, 'submit', 10, 'assemble', 20, 'compile', 30, 'run', 40, 'runtime', 40, 'restart_for_error', 50, 'restart_for_user', 60, 'completion', 70
         , 80) AS rule_type_order
      , war."order", war.profile_name
   FROM
      sys.wlm_active_rule AS war
      LEFT JOIN sys.wlm_active_profile AS wap
         ON (wap.name = war.profile_name)
   WHERE 
      {rule_clause}
      OR war.profile_name = '(global)'
)
SELECT
    ('//Rule Count:' || TO_CHAR(ROW_NUMBER() OVER(ORDER BY superuser, rule_type_order, "order", profile_name), '000')
    || CHR(10) || rule_att || CHR(10))::VARCHAR(60000) AS rule
FROM rule
ORDER BY rule
$str$, '{rule_clause}', _rule_clause);

   --RAISE INFO '_sql: %', _sql;
   
   RETURN QUERY EXECUTE _sql; 

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

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
. _profile_name - (optional) VARCHAR - choose the profile to report on, default is the
                      current active profile

Notes:
. Changes in the current profile are not reflected until saved/activated.

Version:
. 2021.12.09 - Yellowbrick Technical Support 
$str$
;