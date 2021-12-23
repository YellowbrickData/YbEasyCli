/* ****************************************************************************
** proc_name_p()
**
** OneSentenceDescriptionGoesHere.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
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
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**

*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS proc_name_t CASCADE
;

CREATE TABLE proc_name_t
   (

   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE proc_name_p()
   RETURNS SETOF proc_name_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'proc_name_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT
   ...
   ';

   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$
;

   
COMMENT ON FUNCTION proc_name_p( DataTypesIfAny ) IS 
'Description:
1_sentence_summary 

additional_text_if_desired
  
Examples:
  SELECT * FROM proc_name_p( DataTypesIfAny ) 
  
Arguments:
. _arg_name - (optional) arg_description.

Notes:
. general_usage_notes

Version:
. 2021.12.09 - Yellowbrick Technical Support
'
;