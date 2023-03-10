/* query_rule_events_p.sql
**
** Return the WLM rule events for a query.
**
** NOTE:
**
** . This procedure needs to be created by a superuser for privileged users to 
**   all running queries, not only their own.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2023.03.09 - Cosmetic updates
** . 2022.03.24 - Initial Version
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id  |event_order|event_time             |rule_type |rule_name                        |event_type|event                                                    |
**  ----------+-----------+-----------------------+----------+---------------------------------+----------+---------------------------------------------------------+
**  2514544697|          1|2022-03-24 23:58:19.751|submit    |                                 |          |                                                         |
**  2514544697|          2|2022-03-24 23:58:19.751|          |PROD: logSubmit                  |info      |w.resourcePool: null                                     |
**  2514544697|          3|2022-03-24 23:58:19.751|          |global_throttleConcurrentQueries |throttle  |Throttle 500 accesses                                    |
**  2514544697|          4|2022-03-24 23:58:19.752|assemble  |                                 |          |                                                         |
**  2514544697|          5|2022-03-24 23:58:19.752|          |PROD: logAssemble                |info      |w.resourcePool: null                                     |
**  2514544697|          6|2022-03-24 23:58:19.762|prepare   |                                 |          |                                                         |
**  2514544697|          7|2022-03-24 23:58:19.762|          |global_throttleExternalTables    |info      |Rule changed no query settings                           |
**  2514544697|          8|2022-03-24 23:58:19.762|          |global_mapAAToLowPriority        |ignore    |Rule configured for superuser queries (query user: denav)|
**  ...
** 
*/
DROP   TABLE IF EXISTS query_rule_events_t CASCADE ;
CREATE TABLE           query_rule_events_t
(
    query_id      BIGINT
    , event_order BIGINT
    , event_time  TIMESTAMP
    , rule_type   VARCHAR(128)
    , rule_name   VARCHAR(128)
    , event_type  VARCHAR(128)
    , event       VARCHAR(4096)
)
DISTRIBUTE ON ( query_id )
;

 

/* ****************************************************************************
** Create the procedure.
*/

CREATE OR REPLACE PROCEDURE query_rule_events_p(
     _query_id   BIGINT  
   , _rule_type  VARCHAR DEFAULT '' 
   , _event_type VARCHAR DEFAULT '' )
   RETURNS SETOF query_rule_events_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql               TEXT    := '';
   _rule_type_clause  TEXT    := 'TRUE';
   _event_type_clause TEXT    := 'TRUE';

   _fn_name     VARCHAR(256) := 'query_rule_events_p';
   _prev_tags   VARCHAR(256) := current_setting('ybd_query_tags');
   _tags        VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   

BEGIN

   -- SET TRANSACTION       READ ONLY;
   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || ''''; 

   IF _rule_type <> '' THEN
      _rule_type_clause := 'row_rule_type IN ('|| _rule_type || ')';
   END IF;

   IF _event_type <> '' THEN
      _event_type_clause := $str$event_type IN ('begin', $str$ || _event_type || ')';
   END IF;

   _sql := REPLACE(REPLACE(REPLACE($sql$WITH
query AS (
    -- select the query_id of interest
    SELECT {query_id} AS query_id
)
, rules AS (
    SELECT * FROM sys.query_rule_event
    WHERE
        query_id = (SELECT query_id FROM query)
)
, rownum AS (
    SELECT ROW_NUMBER() OVER () AS rownum, *
    FROM rules
)
, grpnum1 AS (
    SELECT
        DECODE(event_type, 'begin', SUBSTR(event, 7, STRPOS(event, ']')-7), NULL) AS rule_type
        , NVL(DECODE(event_type, 'end', LAG(rownum) OVER (), rownum), 1) AS rule_type_grpnum
        , *
    FROM rownum
    WHERE rule_name = 'rule'
)
, grpnum2 AS (
    SELECT
        g1.rownum AS begin_rownum
        , g2.rownum AS end_rownum
        , g1.event_time AS begin_rule_type_time
        , DECODE(g1.event_type, 'begin', SUBSTR(g1.event, 7, STRPOS(g1.event, ']')-7), NULL) AS row_rule_type
        , DECODE(row_rule_type, 'submit', 1, 'assemble', 2, 'prepare', 3, 'compile', 4, 10) AS rule_type_order
    FROM
        grpnum1 AS g1
        JOIN grpnum1 AS g2
            ON g1.rule_type_grpnum = g2.rule_type_grpnum
            AND g1.event_type = 'begin'
            AND g2.event_type = 'end'
)
SELECT
    query_id
    , ROW_NUMBER() OVER (ORDER BY begin_rule_type_time, rule_type_order, begin_rownum, rownum) AS event_order
    , DECODE(event_type, 'begin', event_time, NULL)::TIMESTAMP AS event_time
    , DECODE(event_type, 'begin', grpnum2.row_rule_type, NULL)::VARCHAR(128) AS rule_type
    , DECODE(event_type, 'begin', NULL, rule_name)::VARCHAR(128) AS rule_name
    , DECODE(event_type, 'begin', NULL, event_type)::VARCHAR(128) AS event_type
    , DECODE(event_type, 'begin', NULL, event)::VARCHAR(4096) AS event
FROM
    grpnum2
    JOIN rownum
        ON rownum.rownum >= grpnum2.begin_rownum
        AND rownum.rownum <= grpnum2.end_rownum
WHERE
    event_type != 'end'
    AND {rule_type_clause}
    AND {event_type_clause}
ORDER BY
    begin_rule_type_time, rule_type_order, begin_rownum, rownum
$sql$::VARCHAR(4096), '{query_id}'::VARCHAR, _query_id::VARCHAR), '{rule_type_clause}'::VARCHAR, _rule_type_clause::VARCHAR), '{event_type_clause}'::VARCHAR, _event_type_clause::VARCHAR);
    
   --RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
  
END;
$proc$
;


COMMENT ON FUNCTION query_rule_events_p( BIGINT, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
Return the WLM rule events for a query. 

Examples:
  SELECT * FROM query_rule_events_p(12345);

Arguments:
. _query_id   BIGINT  (reqd) - query_id as a BIGINT.
. _rule_type  VARCHAR (optl) - DEFAULT ''.
. _event_type VARCHAR (optl) - DEFAULT ''.

Revision:
. 2022.03.24 - Yellowbrick Technical Support 
$cmnt$
;