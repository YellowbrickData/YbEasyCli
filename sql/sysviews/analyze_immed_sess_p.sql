/* analyze_immed_sess_p.sql
**
** Disable/enable immediate analyze of user tables from within a session. 
**
** This is useful from within a PLpgSQL function or SQL script.
** If you are running batch jobs from outside the database, it 
** is probably more useful to use analyze_immed_sess_p which 
** enables/disables the property for the user.
**
** This is for use with YBDW 4.x where ANALYZE HLL is now executed
** for every instance of a backend INSERT/UPDATE/DELETE or bulk-load. 
** Not just when there has been a change of x% in data.
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

CREATE OR REPLACE PROCEDURE analyze_immed_sess_p( _off_or_on VARCHAR ) 
RETURNS VOID AS
$$
DECLARE
   
BEGIN
   EXECUTE  'SET ybd_analyze_after_writes  TO ' || _off_or_on ;
END;
$$
LANGUAGE 'plpgsql' 
VOLATILE
SECURITY DEFINER
;


COMMENT ON FUNCTION analyze_immed_sess_p( VARCHAR ) IS 
'Description:
SETs ybd_analyze_after_writes TO [OFF|ON]` for session as a superuser.

The property can only be set by a superuser. This procedure sets the property
only within the current session. If you need it to span sessions, set it at the 
user level using analyze_immed_user_p().
 
Examples:
  SELECT * FROM analyze_immed_sess_p( ''OFF'' );
  CALL analyze_immed_sess_p( $$on$$ );

Arguments:
. _off_or_on - The literal text ''OFF'' or ''ON''.
               Case insensitive.
 
Revision:
. 2020.07.31 - Yellowbrick Technical Support
'
;



