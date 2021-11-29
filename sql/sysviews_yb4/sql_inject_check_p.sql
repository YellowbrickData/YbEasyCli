/* ****************************************************************************
** sql_inject_check_p()
**
** Check for possible sql injection.
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
** . 2021.11.05 - Yellowbrick Technical Support 
*/
CREATE OR REPLACE PROCEDURE sql_inject_check_p(_type VARCHAR, _sql VARCHAR(60000))
    RETURNS BOOLEAN
    LANGUAGE 'plpgsql'
AS $proc$
DECLARE
    _check BOOLEAN;
    _checked_clause TEXT := 'failed';
BEGIN
    SELECT REGEXP_LIKE(_sql, 'SELECT|DELETE|INSERT|UPDATE|;', 1, 1, 'i') INTO _check FROM sys.const;
    IF _check
    THEN
        RAISE EXCEPTION 'SQL clause contains an invalid SQL key word or symbol: %', _sql 
            USING hint = 'possible SQL injection';
    END IF;
    --
    CASE _type
        WHEN '_yb_util_filter' THEN
            SELECT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                _sql, '''[A-Za-z0-9_%.]*''', '')
                , '[A-Za-z0-9_.]*\s(LIKE|NOT LIKE|IN|NOT IN|=)', '')
                , '\s|TRUE|AND|OR|\(|\)|,', '') INTO _checked_clause FROM sys.const;
        ELSE
            RAISE EXCEPTION 'Check ''type'' not recognized.'
                USING hint = 'possible SQL injection';
    END CASE;
    IF 0 != LENGTH(_checked_clause)
    THEN
        RAISE EXCEPTION 'Invalid SQL clause: %', _checked_clause 
            USING hint = 'possible SQL injection';
    END IF;
    RETURN FALSE;
END $proc$;

COMMENT ON FUNCTION sql_inject_check_p(VARCHAR, VARCHAR) IS 
'Description:
Check the input SQL clause for possible SQL injection. 

Examples:
  SELECT all_user_objs_p(''_yb_util_filter'', $a$TRUE AND (database IN (''dze_db1'', ''dze_db2'')$a$) 
  
Arguments:
  _type: is meant for future use to choose the type of check being performed
  _sql: the SQL clause being checked

Version:
. 2021.11.05 - Yellowbrick Technical Support
'
;
