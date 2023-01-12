/* ****************************************************************************
** sysviews_p.sql
**
** Brief listing of the sysviews procedures and their arguments.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018-2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Version History:
** . 2022.07.08 - Added optional _procedure_ilike argument.
**                Fixed INT4 in "arguments" column.
** . 2021.12.09 - ybCliUtils inclusion.
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
CREATE OR REPLACE PROCEDURE sysviews_p( _procedure_ilike VARCHAR DEFAULT '%')
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

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';

   -- The first line of the description text is expected to be the literal text 'Description:'.
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
                                           , ''integer''                    , ''INT4''       )
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
      AND proname ILIKE ' || quote_literal( _procedure_ilike ) || '
   ORDER BY
      1, 2, 3
   ';

   RETURN QUERY EXECUTE _sql ;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;   
$proc$
;

   
COMMENT ON FUNCTION sysviews_p( VARCHAR ) IS 
$comment$Description:
Names and arguments for all installed sysviews procedures.
  
Examples:
  SELECT * FROM sysviews_p() ;
  
Arguments:
. _procedure_ilike (optional) - ILIKE pattern for the procedure name. Default '%'.

Version:
. 2022.07.08 - Yellowbrick Technical Support
$comment$
;