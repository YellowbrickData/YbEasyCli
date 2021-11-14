/* ****************************************************************************
** sysviews_p()
**
** Brief listing of the sysviews procedures and their arguments.
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
** Version History:
** . 2020.10.30 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support
** . 2020.02.16 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  schema | procedure   |                    arguments                    |          returns
** --------+-------------+-------------------------------------------------+----------------------------
**  public | help_p      | _sysviews_like VARCHAR DEFAULT '%'::VARCHAR(1)  | SETOF help_t
**  public | log_query_p | _pred VARCHAR DEFAULT ''::VARCHAR               | SETOF log_query_t
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS sysviews_t CASCADE
;

CREATE TABLE sysviews_t
(
   schema       VARCHAR(128)   
 , procedure    VARCHAR(128)   
 , arguments    VARCHAR(1000) 
 , description  VARCHAR(4000) 
)
;

DROP   PROCEDURE IF EXISTS sysviews_p();

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE sysviews_p()
   RETURNS SETOF sysviews_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql        TEXT := '';
   
   _fn_name    VARCHAR(256) := 'sysviews_p'; 
   _prev_tags  VARCHAR(256) := current_setting('ybd_query_tags');
   _tags       VARCHAR(256) := NVL( _prev_tags, '') || ':sysviews:' || _fn_name;   
     
BEGIN  

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;  

   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   PERFORM _sql ;    

   _sql := 'SELECT
      n.nspname::VARCHAR(128)                                                            AS schema
    , p.proname::VARCHAR(128)                                                            AS procedure
    , (''  '' 
      ||   
      REGEXP_REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
         pg_get_function_arguments (p.oid), ''character varying''           , ''VARCHAR''    )
                                           , ''timestamp without time zone'', ''TIMESTAMP''  ) 
                                           , ''timestamp with time zone''   , ''TIMESTAMPTZ'')                                            
                                           , ''bigint''                     , ''BIGINT''     )
                                           , ''integer''                    , ''INT8''       )
                                           , '',''                          , e''\n,''       )  
                                           , ''(::[A-Z]+\S*)''              , '''', ''g''    ))::VARCHAR(1000)
                                                                                         AS arguments
    , NVL(trim(split_part( d.description, e''\n'', 2)), '''')::VARCHAR(4000)             AS description
   FROM
      pg_catalog.pg_proc                p
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid    = p.pronamespace
      LEFT JOIN pg_description          d ON d.objoid = p.oid      
   WHERE
      pg_catalog.pg_function_is_visible (p.oid)
      AND p.prosp    = ''t''
      AND proname LIKE ''%_p''
   ORDER BY
      1, 2, 3
   ';

   RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   
   
END;   
$proc$
;

-- ALTER FUNCTION sysviews_p( VARCHAR )
--    SET search_path = pg_catalog,pg_temp;
   
COMMENT ON FUNCTION sysviews_p() IS 
'Description:
Names and arguments for all installed sysviews procedures.
  
Examples:
  SELECT * FROM sysviews_p() ;
  
Arguments:
. None

Version:
. 2020.10.30 - Yellowbrick Technical Support
'
;