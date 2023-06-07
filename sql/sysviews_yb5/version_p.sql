/* ****************************************************************************
** version_p()
**
** Current active WLM profile rules.
**
** Usage:
**   See COMMENT ON FUNCTION text further below.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2023-05-23 - Updated version.
** . 2023-04-10 - Updated version.
** . 2023-04-03 - Updated version.
** . 2023-03-20 - Updated version.
** . 2023-03-13 - Updated version.
** . 2023-03-10 - Changed yellowbrick_versions to min and max version
** . 2023-01-11 - Update
** . 2022.12.29 - YbEasyCli inclusion. 
** . 2022.03.05 - Inital Version 
*/

/* ****************************************************************************
**  Example results:
**
**  revision_date | yellowbrick_versions
** ---------------+----------------------
**  2022-08-07    | 5.0 to 5.3
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS version_t CASCADE
;

CREATE TABLE version_t
   (
      revision_date   DATE
    , yb_min_version  VARCHAR( 24 )
    , yb_max_version  VARCHAR( 24 )    
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE version_p()
RETURNS SETOF version_t 
   LANGUAGE 'plpgsql'
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql TEXT := $$SELECT '2023-06-05'::DATE, '5.2'::VARCHAR(24), '5.4'::VARCHAR(24) $$;
    
BEGIN  

   --RAISE INFO '_sql: %', _sql;
   RETURN QUERY EXECUTE _sql; 

END;   
$proc$ 
;


COMMENT ON FUNCTION version_p() IS 
$cmnt$Description:
The current installed sysviews_yb5 version.
  
Examples:
  SELECT * FROM version_p(); 
  
Arguments: 
. None

Version:
. 2023-04-03 - Yellowbrick Technical Support 
$cmnt$
;
