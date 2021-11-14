/* ****************************************************************************
** help_p()
**
** Appliance storage summary by database; committed data space used by database.
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
** . 2021.05.07 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example result:
**
**  -------------------------------------------------------------------------      
** 
**  help_p ("character varying")                                                   
**    returns (help_t)                                                             
**                                                                                 
**  Description:                                                                   
**    Returns usage information on sys.*_p procedures.                             
**                                                                                 
**  Arguments:                                                                     
**  . _proc_name_like - (optional) a procedure name or LIKE pattern to search for. 
**      i.e. 'help_p' or 'log_%'. Default: '%'.                                    
** ...
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS help_t CASCADE
;

CREATE TABLE help_t
   (
      description VARCHAR(64000)
   )
;


/* ****************************************************************************
** Create the procedure.
*/
DROP    PROCEDURE IF EXISTS help_p()
;

CREATE OR REPLACE PROCEDURE help_p( _proc_name_like VARCHAR DEFAULT '%' )
RETURNS SETOF help_t 
LANGUAGE 'plpgsql' 
VOLATILE
CALLED ON NULL INPUT
SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT := '';
   _ret_rec   help_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'help_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      
  
BEGIN  

   --SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;  

   _sql := 'WITH procedures AS
   (  SELECT
         p.oid                                                AS oid
       , TRIM (n.nspname)                                     AS nspname
       , TRIM (p.proname)                                     AS proname
       , p.pronargs                                           AS pronargs
       , p.proargnames                                        AS arg_names
       , array (  SELECT  unnest (p.proargtypes) ::regtype)   AS args_type_names
       , p.prorettype::regtype                                AS ret_type_names
       , p.proretset                                          AS ret_set
       , TRIM (l.lanname)                                     AS lang
       , TRIM (u.usename)                                     AS owner
       , pg_catalog.pg_function_is_visible (p.oid)            AS is_visible
       , p.prosp                                              AS prosp
      FROM
         pg_proc                        p
         LEFT OUTER JOIN pg_description d ON d.objoid       = p.oid
         INNER JOIN pg_namespace        n ON p.pronamespace = n.oid
         INNER JOIN pg_language         l ON l.oid          = p.prolang
         LEFT OUTER JOIN pg_user        u ON u.usesysid     = p.proowner
      WHERE
         n.nspname NOT IN (''pg_catalog'', ''information_schema'', ''sys'')
   )
   SELECT
        p.proname || '' '' || translate( p.args_type_names::varchar(1024), ''{}'',''()'' ) || chr(10)
     || ''  returns ('' || p. ret_type_names || '') '' || chr(10)  
     || chr(10) 
     || NVL( d.description, '''') || chr(10)
     || ''-------------------------------------------------------------------------'' 
     || chr(10)
   FROM
      procedures               p
      LEFT JOIN pg_description d ON d.objoid = p.oid
   WHERE
      p.oid            > 16384
      AND p.proname LIKE ''%\_p''
      AND proname ILIKE ''' || _proc_name_like || '''
   ORDER BY
      p.proname, p.pronargs
   ';

   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;    

END;   
$proc$ 
;

-- ALTER FUNCTION help_p( VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION help_p( VARCHAR ) IS 
'Description:
  Returns usage information on sys *_p procedures.
  
Examples:
  SELECT * FROM help_p();  
  SELECT * FROM help_p( ''help_p'' );    
  SELECT * FROM help_p( ''query%'');  
  
Arguments:
. _proc_name_like - (optional) a procedure name or LIKE pattern to search for. 
    i.e. ''help_p'' or ''log_%''. Default: ''%''.
    
Revision History:
. 2021.05.07 - Yellowbrick Technical Support    
'
;


