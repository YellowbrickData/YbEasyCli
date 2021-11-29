/* analyze_immed_user_p.sql
**
** Run ALTER SYSTEM SET ybd_analyze_after_writes as a superuser.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** Prerequisites:
**   The CREATE PROCEDURE must be run as a superuser.
**
** (c) 2020 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2020.07.31 - Yellowbrick Technical Support 
*/

CREATE OR REPLACE PROCEDURE analyze_immed_user_p( _off_or_on VARCHAR ) 
RETURNS VOID AS
$$
DECLARE
   _sql       TEXT    := '';
   _as_user   VARCHAR := session_user ;
BEGIN

   EXECUTE 'ALTER USER ' || _as_user || ' SET ybd_analyze_after_writes  TO ' || _off_or_on ; 
 
END;
$$
LANGUAGE 'plpgsql' 
VOLATILE
SECURITY DEFINER
;


COMMENT ON FUNCTION analyze_immed_user_p( VARCHAR ) IS 
'Description:
Run ALTER USER session_user SET ybd_analyze_after_writes as a superuser.
 
Examples:
  SELECT * FROM analyze_immed_user_p( ''OFF'' );
  SELECT * FROM analyze_immed_user_p( ''on'' );

Arguments:
. _off_or_on - The literal text ''OFF'' or ''ON''.
               Case insensitive.
 
Revision:
. 2020.05.19 - Yellowbrick Technical Support
'
;
