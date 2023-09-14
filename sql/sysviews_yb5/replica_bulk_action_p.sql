/* ****************************************************************************
** replica_bulk_action_p()
**
** Build SQL script to PAUSE, RESUME, or RESTART replicas.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2023 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
*/

/* ****************************************************************************
**  Example result:
*/
/*  0000001 */ --sleep for  2.00 minutes between RESTART of replicas
/*  0000002 */ --RESTART DB Name: dze_db1, Replica Name: dze_db2_replica
/*  0000003 */ --ALTER DATABASE dze_db1 ALTER REPLICA dze_db2_replica PAUSE;
/*  0000004 */ --ALTER DATABASE dze_db1 ALTER REPLICA dze_db2_replica RESUME;
/*  0000005 */
/*  0000006 */ --SELECT sys.inject_idle(((1000 * 60) * 2)::INT)::INT FROM sys.const;
/*  0000007 */ --RESTART DB Name: dze_db2, Replica Name: dze_db2_replica
/*  0000008 */ --ALTER DATABASE dze_db2 ALTER REPLICA dze_db2_replica PAUSE;
/*  0000009 */ --ALTER DATABASE dze_db2 ALTER REPLICA dze_db2_replica RESUME;
/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS replica_bulk_action_t CASCADE
;

CREATE TABLE replica_bulk_action_t (
   "--SQL" VARCHAR(64000)
);

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE replica_bulk_action_p(
    _action            VARCHAR(10)    DEFAULT 'RESTART'
    , _replicas        VARCHAR(64000) DEFAULT NULL
    , _stagger_minutes FLOAT          DEFAULT 2.0
    , _yb_util_filter  VARCHAR(64000) DEFAULT 'TRUE'
)
    RETURNS SETOF replica_bulk_action_t 
    LANGUAGE plpgsql
AS $proc$
DECLARE
    v_rec             RECORD;
    v_replica         TEXT;
    v_replica_in_list TEXT    := '';
    v_sql             TEXT;
    v_status          TEXT    := $code$status = 'RUNNING'$code$;
    v_line_ct         INT     := 0;
    v_is_first        BOOLEAN := TRUE;
    --
    _fn_name   VARCHAR(256) := 'replica_bulk_action_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
BEGIN
    -- Append sysviews:proc_name to ybd_query_tags
    EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';
    --
    PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);  
    --
    IF (_action IS NOT NULL) AND (_action NOT IN ('PAUSE', 'RESTART', 'RESUME')) THEN
        RAISE EXCEPTION 'error: _action must be one of: PAUSE, RESTART, RESUME';
    END IF;
    --
    DROP TABLE IF EXISTS pg_temp.script_t;
    --
    CREATE TEMP TABLE script_t (
        "--SQL for replica_bulk_action_p" VARCHAR(64000)
    );
    --
    IF _yb_util_filter != 'TRUE' THEN
        v_status = 'TRUE';
    END IF;
    --
    IF LENGTH(_replicas) > 0 THEN
        FOREACH v_replica IN ARRAY string_to_array(_replicas, '|') LOOP
            v_replica_in_list := v_replica_in_list || quote_literal(TRIM(v_replica)) || ',';
        END LOOP;
        v_replica_in_list := 'replica_name IN (' || RTRIM(v_replica_in_list, ',') || ')';
        v_status = 'TRUE';
    ELSE
        v_replica_in_list := 'TRUE';
    END IF;
    --
    v_sql := format(
$code$SELECT d.name AS db_name, r.name AS replica_name
FROM sys.REPLICA AS r JOIN sys.database AS d USING (database_id)
WHERE
    %s
    AND %s
    AND %s
ORDER BY replica_name $code$, v_status, v_replica_in_list, _yb_util_filter);
    --
    FOR v_rec IN EXECUTE v_sql
    LOOP
        --
        --RAISE INFO 'line: %, action: %, db_name: %, replica_name: %', v_line_ct, _action, v_rec.db_name, v_rec.replica_name; --DEBUG
        --
        IF NOT v_is_first THEN
            v_line_ct := v_line_ct + 1;
            INSERT INTO script_t VALUES (format('/*%s */', TO_CHAR(v_line_ct, '0000009'))); 
        END IF;
        --
        IF v_is_first THEN
            IF _action IN ('RESUME', 'RESTART') THEN
                v_line_ct := v_line_ct + 1;
                INSERT INTO script_t VALUES (format('/*%s */ --sleep for %s minutes between %s of replicas', TO_CHAR(v_line_ct, '0000009'), TO_CHAR(_stagger_minutes, '9.99'), _action));
            END IF;
        ELSE
            IF _action IN ('RESUME', 'RESTART') THEN
                v_line_ct := v_line_ct + 1;
                INSERT INTO script_t VALUES (format('/*%s */ SELECT sys.inject_idle(((1000 * 60) * %s)::INT)::INT FROM sys.const;', TO_CHAR(v_line_ct, '0000009'), _stagger_minutes::VARCHAR));
            END IF;
        END IF;
        --
        v_line_ct := v_line_ct + 1;
        INSERT INTO script_t VALUES (format('/*%s */ --%s DB Name: %s, Replica Name: %s', TO_CHAR(v_line_ct, '0000009'), _action, v_rec.db_name, v_rec.replica_name)); 
        --
        IF _action IN ('PAUSE', 'RESTART') THEN
            v_line_ct := v_line_ct + 1;
            INSERT INTO script_t VALUES (format('/*%s */ ALTER DATABASE %s ALTER REPLICA %s PAUSE;', TO_CHAR(v_line_ct, '0000009'), v_rec.db_name, v_rec.replica_name));
        END IF;
        --
        IF _action IN ('RESUME', 'RESTART') THEN
            v_line_ct := v_line_ct + 1;
            INSERT INTO script_t VALUES (format('/*%s */ ALTER DATABASE %s ALTER REPLICA %s RESUME;', TO_CHAR(v_line_ct, '0000009'), v_rec.db_name, v_rec.replica_name));
        END IF;
        --
        v_is_first := FALSE;
    END LOOP;
    --
    RETURN QUERY EXECUTE format('%s', 'SELECT * FROM script_t ORDER BY 1');
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
END
$proc$;

COMMENT ON FUNCTION replica_bulk_action_p( VARCHAR, VARCHAR, FLOAT, VARCHAR ) IS 
$cmnt$Description:
Build SQL script to PAUSE, RESUME, or RESTART replicas.

Examples:
  SELECT * FROM replica_bulk_action_p();
  SELECT * FROM replica_bulk_action_p(_action := 'PAUSE')
  SELECT * FROM replica_bulk_action_p(_action := 'PAUSE', _stagger_minutes := 1.5)
  SELECT * FROM replica_bulk_action_p(_action := 'PAUSE', _stagger_minutes := 1.5, _replicas := 'dze_db1|dze_db2')
  
Arguments:
. _action          - (optional) PAUSE, RESUME, or RESTART; type of SQL script to build
                     The defauuls is 'RESTART'
. _replicas        - (optional) pipe delimited list of replicas to build script for.
                     The default is to build the script for all currently running replicas
. _stagger_minutes - (optional) The number of minutes to wait between RESUME or RESTART of replicas.
                     The default is 2.0 minutes
. _yb_util_filter  - (internal) Used by YbEasyCli.

Version:
. 2023.08.29 - Yellowbrick Technical Support 
$cmnt$
;
