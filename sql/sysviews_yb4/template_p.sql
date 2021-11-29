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
** . 2021.05.10 - Yellowbrick Technical Support 
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
CREATE PROCEDURE proc_name_p( _tbl_name VARCHAR 
                            , _val_name VARCHAR DEFAULT '%' )
RETURNS SETOF proc_name_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   _ret_rec  proc_name_t%ROWTYPE;     
   
   _fn_name   VARCHAR(256) := 'proc_name_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

  /* If using FORMAT instead of quote_literal() and quote_ident():
   ** . s - formats the argument value as a simple string. A null value is treated as an empty string.
   ** . I - treats the argument value as an SQL identifier, double-quoting it if necessary. Must not be NULL.
   ** . L - quotes the argument value as an SQL literal. A null value is displayed as the string NULL, without quotes.
   ** . %%- If you want to include the literal character "%" in the result string, use double percentages %%
   */
   _sql := FORMAT( 'SELECT
   *
   FROM %I
   WHERE some_col = %L
   ', _tbl_name, _val_name
   );

   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;

-- ALTER FUNCTION proc_name_p( DataTypesIfAny )
--    SET search_path = pg_catalog,pg_temp;
   
COMMENT ON FUNCTION proc_name_p( VARCHAR, VARCHAR ) IS 
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
. 2021.05.10 - Yellowbrick Technical Support
'
;



