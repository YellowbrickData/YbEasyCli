-- =============================================================================
-- Yellowbrick Performance Testing Framework (YB-PTF)
-- =============================================================================
--
-- Description:
--   The YB-PTF script deploys a robust, concurrent SQL-based framework for
--   performance testing in a Yellowbrick Data Warehouse. It is targeted at
--   easy of deployment, testing, results analysis, and eliminating environmental
--   factors outside of the database as much as possible.  It allows users to
--   capture a set of queries from system logs, re-run them under different
--   conditions (with true concurrency), and compare performance metrics between
--   runs. This is useful for evaluating the impact of changes such as new
--   indexes, data distribution strategies, or system configuration adjustments.
--
-- Key Features:
--   Everything runs in YB from a handful of stored procedures provided by the
--       framework.
--   The framework is created by running a single SQL script which creates the
--       stored procedures and tables necessary for the tests.
--   There is no client-side code and no dependency on the network connections
--       or client hosts to get accurate test results.
--   The framework provides stored procedures to perform the tasks of setting,
--       running (single or multi-threaded) and performance comparisons of tests.
--   Tests are created from SELECT statements you specify via a filtering
--       predicate against sys.log query. i.e. all select statements in the last
--       week where the user is in some list of users and the application name is
--       in some list of application names.
--   All performance metadata and data (i.e. the results) are stored in the
--       database.
--
-- To setup the framework:
--   Run the perf_test_framwork.sql file as a superuser in every database that
--       you intend to run tests. It will create a set of stored procs and tables
--       in the schema perf_framework.
--
-- =============================================================================
-- Tags JSON Block:
-- =============================================================================
--   Each query executed by the framework is tagged with a JSON block in the
--   `ybd_query_tags` session variable. This block is stored in the query logs
--   and is used to track all relevant metadata for test set, run, and query.
--
--   Example structure for staged (original) queries:
--     {
--       "test_set_name": "<name>",
--       "test_run_name": "--original--",
--       "original_query_id": 0
--     }
--   Example structure for test run queries:
--     {
--       "test_set_name": "<name>",
--       "test_run_name": "<name>",
--       "test_run_id": <bigint>,
--       "original_query_id": <bigint>
--     }
--
--   Note: These are the only fields present in the tag JSON as constructed by the framework code.
--
--   Purpose:
--     - Enables precise tracking and analysis of which test set/run/query each
--       log entry belongs to.
--     - Allows for robust joins and filtering in all framework tables and views.
--     - Ensures all performance metrics and errors are attributable to the
--       correct test context.
--
-- =============================================================================
-- Create, run, and compare a test set
--   1. Stage a new test set by capturing queries:
--        CALL perf_framework.stage_test_set('<test_set_name>', '<where_clause_on_sys_log_query>');
--      Example:
--        CALL perf_framework.stage_test_set('daily_reports', 'username = ''reporting_user'' AND submit_time > NOW() - INTERVAL ''1 DAY'''); 
--
--   2. List all available test sets:
--        CALL perf_framework.show_test_sets();
--
--   3. List all runs for a specific test set:
--        CALL perf_framework.show_test_runs('<test_set_name>');
--      Example:
--        CALL perf_framework.show_test_runs('daily_reports');
--
--   4. Prepare a concurrent test run (creates a new run and populates the work queue):
--        CALL perf_framework.prepare_test_run('<test_set_name>', '<test_run_name>');
--      Example:
--        CALL perf_framework.prepare_test_run('daily_reports', 'concurrent_run_1');
--
--   5. Launch N concurrent sessions to execute the test run (one per thread):
--        ybsql -c "CALL perf_framework.run_test_thread('<test_set_name>', '<test_run_name>');" 2>&1 | grep -E '^(INFO|ERROR)' &
--      Example:
--        ybsql -c "CALL perf_framework.run_test_thread('daily_reports', 'concurrent_run_1');" 2>&1 | grep -E '^(INFO|ERROR)' &
--
--   Note: Use unique test_set_name and test_run_name values for each test set/run to avoid log matching issues.
--
--   6. Compare the performance of two runs:
--        CALL perf_framework.compare_runs('<test_set_name>', '<run_name_1>', '<run_name_2>');
--      Example:
--        CALL perf_framework.compare_runs('daily_reports', '--original--', 'concurrent_run_1');
--
--   7. Review the comparison results:
--        SELECT * FROM tmp_aggregate_comparison;
--
-- =============================================================================
-- Notes, Caveats and Limitations
--   We have striped this framework down to the fundamentals to be as simple as
--   possible. We expect it might need to be extended which we or the user can
--   do as needs require. So:
--
--   - Only SELECT statements are supported in this initial revision.
--   - By design, all SELECTS are to run as CTAS statements; this eliminates the
--       client and network interaction in the results.
--   - The set of queries in a test set will be executed against a single
--       database. If you need to execute statements against multiple databases,
--       you can create different test sets that you invoke serially or
--       concurrently.
--   - If user of the original query execution has a search_path other than the
--       system default, the session's search_path must be set in the session
--       before calling the test run stored procedure.
--   - Statements will be executed as the original statement user using `SET
--       ROLE`. So the users specific properties, if any, will not be picked up.
--   - However, the original statement’s ybd_query_tags will be kept. You can
--       also use planner hints via WLM rules.
--   - You cannot bind a set of statements in a multi-threaded test to a specific
--       thread. For example, the order dependent set of statements CREATE TABLE
--       t2 AS SELECT * FROM t1; SELECT * FROM t2 WHERE col1 ='x'; DROP t2;
--       would not be guaranteed to execute in the same thread in a
--       multi-threaded test.
--   - An UDATE statement is executed before each test query invocation (as part
--       of the mechanism that enables multi-threaded execution of statements in
--       the same test set.

DROP SCHEMA IF EXISTS perf_framework CASCADE;
CREATE SCHEMA perf_framework;
GRANT USAGE ON SCHEMA perf_framework TO public;
ALTER DEFAULT PRIVILEGES IN SCHEMA perf_framework
GRANT ALL PRIVILEGES ON PROCEDURES TO public;

DROP PROCEDURE IF EXISTS perf_framework.create_framework_objects();

-- =============================================================================
-- Stored Procedure: create_framework_objects
-- Description:
--   Sets up the necessary database objects (tables, sequences, views) for the framework.
--   This procedure is idempotent and can be re-run to reset the schema. It will drop and recreate all objects listed below.
--
--   Creates:
--     Tables:
--       - perf_framework.query_history
--       - perf_framework.query_history_text
--       - perf_framework.work_queue
--       - perf_framework.test_run_finalization
--     Sequences:
--       - perf_framework.work_queue_id_seq
--       - perf_framework.thread_id_seq
--     Views:
--       - perf_framework.v_query_history_consolidated
-- =============================================================================
CREATE OR REPLACE PROCEDURE perf_framework.create_framework_objects()
 RETURNS void
 LANGUAGE plpgsql
AS $proc$
BEGIN
    RAISE INFO '-- Creating performance framework tables and views...';

    -- Table to store the historical queries and their performance metrics.
    DROP TABLE IF EXISTS perf_framework.query_history;
    CREATE TABLE perf_framework.query_history (
        test_set_id         BIGINT,
        test_run_id         BIGINT,
        is_original_run     BOOLEAN,
        query_id            BIGINT NOT NULL,
        session_id          BIGINT NOT NULL,
        transaction_id      BIGINT,
        plan_id             VARCHAR(64),
        state               VARCHAR(50) NOT NULL,
        username            VARCHAR(128) NOT NULL,
        application_name    VARCHAR(128) NOT NULL,
        database_name       VARCHAR(128) NOT NULL,
        type                VARCHAR(128),
        tags                VARCHAR(512),
        orig_tag            VARCHAR(512),
        error_code          VARCHAR(5),
        error_message       VARCHAR(255),
        pool_id             VARCHAR(128),
        priority            VARCHAR(30),
        slot                BIGINT,
        num_workers         INTEGER,
        longest_worker_id   VARCHAR(38),
        compile_percent     INTEGER,
        cpu_percent         INTEGER,
        cpu_percent_max     INTEGER,
        num_restart         INTEGER NOT NULL,
        num_error           INTEGER NOT NULL,
        parse_ms            NUMERIC(18,3),
        wait_parse_ms       NUMERIC(18,3),
        wait_lock_ms        NUMERIC(18,3),
        plan_ms             NUMERIC(18,3),
        wait_plan_ms        NUMERIC(18,3),
        assemble_ms         NUMERIC(18,3),
        wait_assemble_ms    NUMERIC(18,3),
        compile_ms          NUMERIC(18,3),
        wait_compile_ms     NUMERIC(18,3),
        acquire_resources_ms NUMERIC(18,3),
        run_ms              NUMERIC(18,3),
        wait_run_cpu_ms     NUMERIC(18,3),
        wait_run_io_ms      NUMERIC(18,3),
        wait_run_spool_ms   NUMERIC(18,3),
        client_ms           NUMERIC(18,3),
        wait_client_ms      NUMERIC(18,3),
        total_ms            NUMERIC(18,3),
        cancel_ms           NUMERIC(18,3),
        restart_ms          NUMERIC(18,3),
        wlm_runtime_ms      NUMERIC(18,3),
        spool_ms            NUMERIC(18,3),
        submit_time         TIMESTAMP WITH TIME ZONE NOT NULL,
        done_time           TIMESTAMP WITH TIME ZONE NOT NULL,
        state_time          TIMESTAMP WITH TIME ZONE NOT NULL,
        restart_time        TIMESTAMP WITH TIME ZONE,
        io_read_bytes       BIGINT,
        io_write_bytes      BIGINT,
        io_spill_read_bytes BIGINT,
        io_spill_write_bytes BIGINT,
        io_network_bytes    BIGINT,
        io_client_read_bytes BIGINT,
        io_client_write_bytes BIGINT,
        io_spool_write_bytes BIGINT,
        rows_inserted       BIGINT,
        rows_deleted        BIGINT,
        rows_returned       BIGINT,
        memory_bytes        BIGINT,
        memory_bytes_max    BIGINT,
        io_spill_space_bytes BIGINT,
        io_spill_space_bytes_max BIGINT,
        io_spill_space_granted_bytes BIGINT,
        memory_estimated_bytes BIGINT,
        memory_required_bytes BIGINT,
        memory_granted_bytes BIGINT,
        memory_estimate_confidence VARCHAR(16)
    );
    COMMENT ON TABLE perf_framework.query_history IS 'Stores query logs for original and test runs, forming the core of the performance framework.';

    -- Table to store the query text associated with the history table.
    DROP TABLE IF EXISTS perf_framework.query_history_text;
    CREATE TABLE perf_framework.query_history_text (
        plan_id             VARCHAR(64) NOT NULL,
        query_text          VARCHAR(60000),
        text_index          INT2 NOT NULL,
        PRIMARY KEY (plan_id, text_index)
    );
    COMMENT ON TABLE perf_framework.query_history_text IS 'Stores the full text of queries logged in the query_history table.';

    -- Sequence for work_queue_id
    DROP SEQUENCE IF EXISTS perf_framework.work_queue_id_seq;
    CREATE SEQUENCE perf_framework.work_queue_id_seq START WITH 1;

    -- Table to manage the queue of queries for concurrent execution.
    DROP TABLE IF EXISTS perf_framework.work_queue;
    CREATE TABLE perf_framework.work_queue (
        work_queue_id       BIGINT PRIMARY KEY DEFAULT NEXTVAL('perf_framework.work_queue_id_seq'),
        test_run_id         BIGINT,
        original_query_id   BIGINT,
        username            VARCHAR(128),
        test_set_name       VARCHAR(255),
        test_run_name       VARCHAR(255),
        status              VARCHAR(20) DEFAULT 'queued', -- queued, running, complete, failed
        execution_thread_id INT,
        start_time          TIMESTAMP WITH TIME ZONE,
        end_time            TIMESTAMP WITH TIME ZONE
    );
    COMMENT ON TABLE perf_framework.work_queue IS 'Manages the queue of queries for concurrent test execution.';

    -- Sequence for thread_id assignment
    DROP SEQUENCE IF EXISTS perf_framework.thread_id_seq;
    CREATE SEQUENCE perf_framework.thread_id_seq START WITH 1;
    COMMENT ON SEQUENCE perf_framework.thread_id_seq IS 'Generates unique thread ids for concurrent test execution.';

    -- A consolidated view joining history and text for easy analysis.
    CREATE OR REPLACE VIEW perf_framework.v_query_history_consolidated AS
    SELECT
        h.*,
        t.query_text
    FROM
        perf_framework.query_history h
    LEFT JOIN
        perf_framework.query_history_text t ON h.plan_id = t.plan_id AND t.text_index = 0;
    COMMENT ON VIEW perf_framework.v_query_history_consolidated IS 'A consolidated view of query history with full query text for analysis.';
    GRANT ALL ON perf_framework.query_history TO public;

    -- Table to mark test run finalization (only one thread can archive)
    DROP TABLE IF EXISTS perf_framework.test_run_finalization;
    CREATE TABLE perf_framework.test_run_finalization (
        test_run_id BIGINT PRIMARY KEY,
        archived BOOLEAN DEFAULT FALSE,
        finalized_by_thread INT,
        finalized_at TIMESTAMP WITH TIME ZONE
    );
    COMMENT ON TABLE perf_framework.test_run_finalization IS 'Ensures only one thread archives a test run.';

    RAISE INFO '-- Framework objects created successfully.';
END;
$proc$
;

CALL perf_framework.create_framework_objects();

-- =============================================================================
-- INTERNAL PROCEDURES (SECURITY DEFINER)
-- These procedures encapsulate all access to sys.* objects and run with
-- elevated privileges. They are not intended to be called by end-users.
-- =============================================================================

-- =============================================================================
-- Stored Procedure: _internal_stage_from_sys_log (INTERNAL)
-- Description:
--   Copies query data from `sys.log_query` into the history table based on
--   a user-provided WHERE clause.
--
-- How it Works:
--   A subquery first filters `sys.log_query` to include only relevant query
--   types (`select`, `ctas`). The user's WHERE clause is then applied to
--   this pre-filtered set.
-- =============================================================================
CREATE OR REPLACE PROCEDURE perf_framework._internal_stage_from_sys_log(p_test_set_id BIGINT, p_tag_json VARCHAR, p_where_clause VARCHAR)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $proc$
DECLARE
    v_dynamic_sql VARCHAR(60000);
BEGIN
    SET application_name = 'perf_framework';
    v_dynamic_sql := '
        INSERT INTO perf_framework.query_history
        SELECT '
            || p_test_set_id || ' AS test_set_id,
            NULL AS test_run_id,
            TRUE AS is_original_run,
            q.query_id, q.session_id, q.transaction_id, q.plan_id, q.state, q.username, q.application_name, q.database_name, q."type", ''' || p_tag_json || ''' AS tags, q.tags AS orig_tag, q.error_code, q.error_message, q.pool_id, q.priority, q.slot, q.num_workers, q.longest_worker_id, q.compile_percent, q.cpu_percent, q.cpu_percent_max, q.num_restart, q.num_error, q.parse_ms, q.wait_parse_ms, q.wait_lock_ms, q.plan_ms, q.wait_plan_ms, q.assemble_ms, q.wait_assemble_ms, q.compile_ms, q.wait_compile_ms, q.acquire_resources_ms, q.run_ms, q.wait_run_cpu_ms, q.wait_run_io_ms, q.wait_run_spool_ms, q.client_ms, q.wait_client_ms, q.total_ms, q.cancel_ms, q.restart_ms, q.wlm_runtime_ms, q.spool_ms, q.submit_time, q.done_time, q.state_time, q.restart_time, q.io_read_bytes, q.io_write_bytes, q.io_spill_read_bytes, q.io_spill_write_bytes, q.io_network_bytes, q.io_client_read_bytes, q.io_client_write_bytes, q.io_spool_write_bytes, q.rows_inserted, q.rows_deleted, q.rows_returned, q.memory_bytes, q.memory_bytes_max, q.io_spill_space_bytes, q.io_spill_space_bytes_max, q.io_spill_space_granted_bytes, q.memory_estimated_bytes, q.memory_required_bytes, q.memory_granted_bytes, q.memory_estimate_confidence
        FROM (
            SELECT * FROM sys.log_query WHERE type IN (''select'', ''ctas'')
        ) q';

    IF p_where_clause IS NOT NULL AND TRIM(p_where_clause) <> '' THEN
        v_dynamic_sql := v_dynamic_sql || ' WHERE ' || p_where_clause;
    ELSE
        RAISE EXCEPTION '-- A WHERE clause must be provided to stage a test set.';
    END IF;

    EXECUTE v_dynamic_sql;
END;
$proc$;

-- =============================================================================
-- Stored Procedure: _internal_archive_from_sys_log (INTERNAL)
-- Description:
--   Copies query log entries for a specific test run from `sys.log_query`
--   into the framework's history table.
-- =============================================================================
CREATE OR REPLACE PROCEDURE perf_framework._internal_archive_from_sys_log(p_test_set_id BIGINT, p_test_run_id BIGINT)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $proc$
DECLARE
    v_dynamic_sql VARCHAR(60000);
BEGIN
    SET application_name = 'perf_framework';
    v_dynamic_sql := '
        INSERT INTO perf_framework.query_history
        SELECT '
            || p_test_set_id || ', '
            || p_test_run_id || ', '
            || 'FALSE,
            query_id, session_id, transaction_id, plan_id, state, username, application_name, database_name, "type", tags, NULL AS orig_tag, error_code, error_message, pool_id, priority, slot, num_workers, longest_worker_id, compile_percent, cpu_percent, cpu_percent_max, num_restart, num_error, parse_ms, wait_parse_ms, wait_lock_ms, plan_ms, wait_plan_ms, assemble_ms, wait_assemble_ms, compile_ms, wait_compile_ms, acquire_resources_ms, run_ms, wait_run_cpu_ms, wait_run_io_ms, wait_run_spool_ms, client_ms, wait_client_ms, total_ms, cancel_ms, restart_ms, wlm_runtime_ms, spool_ms, submit_time, done_time, state_time, restart_time, io_read_bytes, io_write_bytes, io_spill_read_bytes, io_spill_write_bytes, io_network_bytes, io_client_read_bytes, io_client_write_bytes, io_spool_write_bytes, rows_inserted, rows_deleted, rows_returned, memory_bytes, memory_bytes_max, io_spill_space_bytes, io_spill_space_bytes_max, io_spill_space_granted_bytes, memory_estimated_bytes, memory_required_bytes, memory_granted_bytes, memory_estimate_confidence
        FROM
            sys.log_query q
        WHERE
            q.tags LIKE ''%"test_run_id": ' || p_test_run_id || '%'' AND type IN (''select'', ''ctas'')';

    EXECUTE v_dynamic_sql;
END;
$proc$;

-- =============================================================================
-- Stored Procedure: _internal_copy_query_text (INTERNAL)
-- Description:
--   Copies the full query text from `sys._log_query_text` into the
--   framework's text history table, avoiding duplicate entries.
-- =============================================================================
CREATE OR REPLACE PROCEDURE perf_framework._internal_copy_query_text(p_id_type VARCHAR, p_id BIGINT)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $proc$
DECLARE
    v_where_clause VARCHAR(100);
BEGIN
    SET application_name = 'perf_framework';
    IF p_id_type = 'test_set' THEN
        v_where_clause := 'h.test_set_id = ' || p_id;
    ELSIF p_id_type = 'test_run' THEN
        v_where_clause := 'h.test_run_id = ' || p_id;
    ELSE
        RAISE EXCEPTION '-- Invalid ID type specified for _internal_copy_query_text. Must be ''test_set'' or ''test_run''.';
    END IF;

    EXECUTE '
        INSERT INTO perf_framework.query_history_text
        SELECT DISTINCT
            qt.plan_id,
            qt.query_text,
            qt.text_index
        FROM
            sys._log_query_text qt
        JOIN
            perf_framework.query_history h ON qt.plan_id = h.plan_id
        WHERE
            (' || v_where_clause || ')
            -- Check to ensure the plan_id does not already exist in the destination table
            AND NOT EXISTS (
                SELECT 1
                FROM perf_framework.query_history_text existing_qt
                WHERE existing_qt.plan_id = qt.plan_id
            )';
END;
$proc$;

-- =============================================================================
-- Stored Procedure: _internal_wait_for_logs (INTERNAL)
-- Description:
--   Pauses execution to wait for query logs to appear in `sys.log_query`,
--   handling potential system logging lag.
-- =============================================================================
CREATE OR REPLACE PROCEDURE perf_framework._internal_wait_for_logs(p_test_run_id BIGINT, p_expected_count BIGINT)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $proc$
DECLARE
    v_logged_query_count BIGINT := 0;
    v_loop_counter INT := 0;
    v_sql VARCHAR(60000);
    -- Infinite loop until all logs are found
BEGIN
    SET application_name = 'perf_framework';
    RAISE INFO '-- Waiting for query logs to appear in sys.log_query...';
    v_sql := 'SELECT count(*) FROM sys.log_query WHERE tags LIKE ''%"test_run_id": ' || p_test_run_id || '%'' AND type IN (''select'', ''ctas'')';
    LOOP
        EXECUTE v_sql INTO v_logged_query_count;

        IF v_logged_query_count >= p_expected_count THEN
            RAISE INFO '-- All % query logs found.', v_logged_query_count;
            EXIT;
        END IF;

        RAISE INFO '-- Waiting for query logs to populate... (% remaining of %)...', v_logged_query_count, p_expected_count;
        PERFORM sys.sleep(2000);
        v_loop_counter := v_loop_counter + 1;
    END LOOP;

    IF v_logged_query_count < p_expected_count THEN
        RAISE WARNING '-- Timed out waiting for all query logs to appear. Archiving what was found (found % of % expected).', v_logged_query_count, p_expected_count;
    END IF;
END;
$proc$;

-- =============================================================================
-- PUBLIC-FACING PROCEDURES
-- =============================================================================

-- =============================================================================
-- Stored Procedure: archive_test_run
-- Description:
--   Public-facing procedure to archive the results of a completed test run.
-- =============================================================================
DROP PROCEDURE IF EXISTS perf_framework.archive_test_run(BIGINT, BIGINT);
CREATE OR REPLACE PROCEDURE perf_framework.archive_test_run(p_test_set_id BIGINT, p_test_run_id BIGINT)
 RETURNS void
 LANGUAGE plpgsql
AS $proc$
BEGIN
    SET application_name = 'perf_framework';
    RAISE INFO '-- Archiving results for Test Set ID: % and Test Run ID: %', p_test_set_id, p_test_run_id;
    EXECUTE 'CALL perf_framework._internal_archive_from_sys_log(' || p_test_set_id || ', ' || p_test_run_id || ');';
    EXECUTE 'CALL perf_framework._internal_copy_query_text(''test_run'', ' || p_test_run_id || ');';
    RAISE INFO '-- Archiving complete.';
END;
$proc$
;

-- =============================================================================
-- Stored Procedure: stage_test_set
-- Description:
--   Captures a new set of queries from `sys.log_query` to serve as a baseline
--   for performance testing.
--
-- Arguments:
--   p_test_set_name (VARCHAR): A unique, descriptive name for the new test set.
--   p_where_clause (VARCHAR): The SQL WHERE clause used to filter queries
--     from `sys.log_query`.
-- =============================================================================
DROP PROCEDURE IF EXISTS perf_framework.stage_test_set(varchar, varchar);
CREATE OR REPLACE PROCEDURE perf_framework.stage_test_set(p_test_set_name character varying, p_where_clause character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $proc$
DECLARE
    v_test_set_id BIGINT;
    v_tag_json VARCHAR(512);
    v_existing_count BIGINT;
    v_staged_query_count BIGINT;
BEGIN
    SET application_name = 'perf_framework';
    -- Check for existing test set name to ensure uniqueness
    EXECUTE 'SELECT count(*) FROM perf_framework.query_history WHERE tags LIKE ''%"test_set_name": "' || REPLACE(p_test_set_name, '''', '''''') || '"%'''
    INTO v_existing_count;
    IF v_existing_count > 0 THEN
        RAISE EXCEPTION '-- A test set with the name "%" already exists. Please choose a unique name.', p_test_set_name;
    END IF;

    v_test_set_id := (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
    v_tag_json := '{"test_set_name": "' || REPLACE(p_test_set_name, '''', '''''') || '", "test_run_name": "--original--", "original_query_id": 0}';

    RAISE INFO '-- Staging new test set. ID: %, Name: %', v_test_set_id, p_test_set_name;

    EXECUTE 'CALL perf_framework._internal_stage_from_sys_log(' || v_test_set_id || ', ' || quote_literal(v_tag_json) || ', ' || quote_literal(p_where_clause) || ');';

    EXECUTE 'SELECT count(*) FROM perf_framework.query_history WHERE test_set_id = ' || v_test_set_id INTO v_staged_query_count;
    EXECUTE 'CALL perf_framework._internal_copy_query_text(''test_set'', ' || v_test_set_id || ');';

    RAISE INFO '-- Staging complete. % queries staged for test set "%".', v_staged_query_count, p_test_set_name;
END;
$proc$
;

-- =============================================================================
-- Procedure: prepare_test_run
-- Description:
--   Prepares a concurrent test run by creating a new test_run_id and populating work_queue
--   with all queries from the specified test set, status='queued'.
--   Outputs suggested commands for each thread.
-- =============================================================================
DROP PROCEDURE IF EXISTS perf_framework.prepare_test_run(VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE perf_framework.prepare_test_run(
    p_test_set_name VARCHAR,
    p_test_run_name VARCHAR
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_test_set_id BIGINT;
    v_test_run_id BIGINT;
    v_existing_run_count BIGINT;
    v_query_rec RECORD;
    v_thread INT;
    v_query_count BIGINT := 0;
BEGIN
    SET application_name = 'perf_framework';
    -- Find the test set ID
    EXECUTE 'SELECT MAX(h.test_set_id) FROM perf_framework.query_history h WHERE h.tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND h.is_original_run = TRUE'
    INTO v_test_set_id;
    IF v_test_set_id IS NULL THEN
        RAISE EXCEPTION '-- Test set with name "%" not found. Please stage it first.', p_test_set_name;
    END IF;

    -- Check for existing run name within this test set to ensure uniqueness
    EXECUTE 'SELECT count(*) FROM perf_framework.query_history WHERE tags LIKE ''%"test_set_name": "' || REPLACE(p_test_set_name, '''', '''''') || '"%'' AND tags LIKE ''%"test_run_name": "' || REPLACE(p_test_run_name, '''', '''''') || '"%'''
    INTO v_existing_run_count;
    IF v_existing_run_count > 0 THEN
        RAISE EXCEPTION '-- A test run with the name "%" already exists for test set "%" in the archive. Please choose a unique name.', p_test_run_name, p_test_set_name;
    END IF;
    -- Also check work_queue for in-progress runs by test_set_name and test_run_name
    SELECT count(*) INTO v_existing_run_count
    FROM perf_framework.work_queue
    WHERE test_set_name = p_test_set_name AND test_run_name = p_test_run_name;
    IF v_existing_run_count > 0 THEN
        RAISE EXCEPTION '-- A test run with the name "%" already exists for test set "%" in the work queue. Please choose a unique name.', p_test_run_name, p_test_set_name;
    END IF;

    v_test_run_id := (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
    RAISE INFO '-- Preparing concurrent run for Test Set: "%" (ID: %). Run Name: "%", Run ID: %', p_test_set_name, v_test_set_id, p_test_run_name, v_test_run_id;

    -- Populate work_queue with all queries from the test set
    INSERT INTO perf_framework.work_queue (test_run_id, original_query_id, username, test_set_name, test_run_name, status)
    SELECT v_test_run_id, query_id, username, p_test_set_name, p_test_run_name, 'queued'
    FROM perf_framework.v_query_history_consolidated
    WHERE test_set_id = v_test_set_id AND is_original_run = TRUE
    ORDER BY submit_time;
    GET DIAGNOSTICS v_query_count = ROW_COUNT;
    -- Seed the finalization table for this run
    INSERT INTO perf_framework.test_run_finalization (test_run_id) VALUES (v_test_run_id);

    RAISE INFO '-- Populated work_queue with % queries for concurrent run.', v_query_count;
    RAISE INFO '--';
    RAISE INFO '-- To execute this test set concurrently, open X ybsql sessions/threads and run the following command in each:';
    RAISE INFO '-- ybsql -c "CALL perf_framework.run_test_thread(''%'', ''%'');" 2>&1 | grep -E ''^(INFO|ERROR)'' &', p_test_set_name, p_test_run_name;
END;
$proc$;

-- =============================================================================
-- Procedure: execute_test_query_and_update_status
-- Description: Executes a test query as CTAS, updates work_queue status, and handles exceptions.
CREATE OR REPLACE PROCEDURE perf_framework.execute_test_query_and_update_status(
    p_test_set_id BIGINT,
    p_test_run_id BIGINT,
    p_test_set_name VARCHAR,
    p_test_run_name VARCHAR,
    p_original_query_id BIGINT,
    p_username VARCHAR,
    p_query_text VARCHAR,
    p_work_queue_id BIGINT,
    p_thread_id INT
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    tag_json VARCHAR(512);
    set_tag_sql VARCHAR(600);
    temp_table_name VARCHAR(256);
    ctas_sql VARCHAR(60000);
BEGIN
    SET application_name = 'perf_framework';
    BEGIN
        EXECUTE 'SET SESSION AUTHORIZATION ' || quote_ident(p_username);
        -- For non-original runs, include test_run_id in the tag JSON
        tag_json := '{"test_set_name": "' || REPLACE(p_test_set_name, '''', '''''') ||
                    '", "test_run_name": "' || REPLACE(p_test_run_name, '''', '''''') ||
                    '", "test_run_id": ' || p_test_run_id ||
                    ', "original_query_id": ' || p_original_query_id || '}';
        set_tag_sql := 'SET ybd_query_tags=' || quote_literal(tag_json) || ';';
        EXECUTE set_tag_sql;
        temp_table_name := 'perf_test_temp_' || p_test_run_id || '_' || p_original_query_id;
        ctas_sql := 'CREATE TEMPORARY TABLE ' || temp_table_name || ' AS ' || p_query_text;
        RAISE INFO '-- Thread %: Executing as CTAS (User: %, Original ID: %): %', p_thread_id, p_username, p_original_query_id, LEFT(p_query_text, 100) || '...';
        EXECUTE ctas_sql;
        EXECUTE 'SET ybd_query_tags = '''';';
        EXECUTE 'DROP TABLE ' || temp_table_name;
        EXECUTE 'RESET SESSION AUTHORIZATION';
        -- Mark as complete
        UPDATE perf_framework.work_queue
        SET status = 'complete', end_time = NOW()
        WHERE work_queue_id = p_work_queue_id;
    EXCEPTION WHEN OTHERS THEN
        EXECUTE 'SET ybd_query_tags = '''';';
        RAISE WARNING '-- Query (User: %, Original ID: %) failed to execute as CTAS: %', p_username, p_original_query_id, SQLERRM;
        EXECUTE 'RESET SESSION AUTHORIZATION';
        UPDATE perf_framework.work_queue
        SET status = 'failed', end_time = NOW()
        WHERE work_queue_id = p_work_queue_id;
        RAISE INFO '-- Thread %: Marked work_queue_id % as failed.', p_thread_id, p_work_queue_id;
    END;
END;
$proc$;

-- =============================================================================
-- Procedure: run_test_thread
-- Description:
--   Worker procedure for concurrent execution. Each session calls this with test set and run name.
--   Atomically claims and executes queued queries from work_queue for the given test_run_id.
--   Thread id is now assigned automatically from a sequence.
-- =============================================================================
DROP PROCEDURE IF EXISTS perf_framework.run_test_thread(VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE perf_framework.run_test_thread(
    p_test_set_name VARCHAR,
    p_test_run_name VARCHAR
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_test_set_id BIGINT;
    v_test_run_id BIGINT;
    v_queue_rec RECORD;
    v_query_rec RECORD;
    v_done BOOLEAN := FALSE;
    v_rowcount INT;
    v_actual_test_set_name VARCHAR;
    v_actual_test_run_name VARCHAR;
    v_query_count BIGINT := 0;
    v_start_time TIMESTAMP WITH TIME ZONE := clock_timestamp();
    v_end_time TIMESTAMP WITH TIME ZONE;
    v_total_time INTERVAL;
    v_thread_id INT;
BEGIN
    SET application_name = 'perf_framework';
    -- Look up IDs from names
    SELECT MAX(test_set_id) INTO v_test_set_id
    FROM perf_framework.query_history
    WHERE tags LIKE '%"test_set_name": "' || p_test_set_name || '"%' AND is_original_run = TRUE;
    IF v_test_set_id IS NULL THEN
        RAISE EXCEPTION '-- Test set with name "%" not found.', p_test_set_name;
    END IF;
    SELECT MAX(test_run_id) INTO v_test_run_id
    FROM perf_framework.query_history
    WHERE test_set_id = v_test_set_id AND tags LIKE '%"test_run_name": "' || p_test_run_name || '"%';

    IF v_test_run_id IS NULL THEN
        -- Try to find in work_queue (in-progress run)
        SELECT MAX(test_run_id) INTO v_test_run_id
        FROM perf_framework.work_queue
        WHERE test_set_name = p_test_set_name AND test_run_name = p_test_run_name;
        IF v_test_run_id IS NULL THEN
            RAISE EXCEPTION '-- Test run with name "%" not found for test set "%".', p_test_run_name, p_test_set_name;
        END IF;
    END IF;

    v_actual_test_set_name := p_test_set_name;
    v_actual_test_run_name := p_test_run_name;

    -- Assign a unique thread id from the sequence
    SELECT NEXTVAL('perf_framework.thread_id_seq') INTO v_thread_id;
    RAISE INFO '-- Thread %: Starting work for run_name "%" (run_id %)', v_thread_id, p_test_run_name, v_test_run_id;
    WHILE TRUE LOOP
        -- Try to claim the next available row
        UPDATE perf_framework.work_queue
        SET status = 'running', execution_thread_id = v_thread_id, start_time = NOW()
        WHERE work_queue_id = (
            SELECT work_queue_id
            FROM perf_framework.work_queue
            WHERE test_run_id = v_test_run_id AND status = 'queued'
            ORDER BY work_queue_id
            LIMIT 1
        )
        AND status = 'queued';
        COMMIT;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_rowcount = 0 THEN
            RAISE INFO '-- Thread %: No more work to claim, exiting loop.', v_thread_id;
            EXIT;
        END IF;

        SELECT
            w.work_queue_id,
            w.original_query_id,
            w.username,
            w.test_set_name,
            w.test_run_name,
            v.query_text
        INTO v_queue_rec
        FROM perf_framework.work_queue w
        JOIN perf_framework.v_query_history_consolidated v
          ON w.original_query_id = v.query_id
             AND v.test_set_id = (SELECT v_test_set_id)
             AND v.is_original_run = TRUE
        WHERE w.status = 'running'
          AND w.execution_thread_id = v_thread_id
          AND w.test_run_id = v_test_run_id
        ORDER BY w.start_time DESC
        LIMIT 1;

        EXECUTE 'CALL perf_framework.execute_test_query_and_update_status(' ||
            v_test_set_id || ', ' ||
            v_test_run_id || ', ' ||
            quote_literal(v_queue_rec.test_set_name) || ', ' ||
            quote_literal(v_queue_rec.test_run_name) || ', ' ||
            v_queue_rec.original_query_id || ', ' ||
            quote_literal(v_queue_rec.username) || ', ' ||
            quote_literal(v_queue_rec.query_text) || ', ' ||
            v_queue_rec.work_queue_id || ', ' ||
            v_thread_id ||
        ')';
        v_query_count := v_query_count + 1;
    END LOOP;
    v_end_time := clock_timestamp();
    v_total_time := v_end_time - v_start_time;
    RAISE INFO '-- Thread %: All queued queries processed for test_run_name "%". Queries run: %, Total time: %', v_thread_id, p_test_run_name, v_query_count, v_total_time;
    -- Only one thread will archive: use update on finalization table
    UPDATE perf_framework.test_run_finalization
    SET archived = TRUE, finalized_by_thread = v_thread_id, finalized_at = clock_timestamp()
    WHERE test_run_id = v_test_run_id AND archived = FALSE;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    COMMIT;
    IF v_rowcount = 1 THEN
        EXECUTE 'CALL perf_framework.wait_and_archive_test_run(''' || p_test_set_name || ''', ''' || p_test_run_name || ''')';
    END IF;
END;
$proc$;

-- =============================================================================
-- Stored Procedure: compare_runs
-- Description:
--   Compares two performance test runs.
--
-- How it Works:
--   Calculates an aggregate summary of performance metrics for both runs
--   and stores the result in `tmp_aggregate_comparison`.
--   For detailed, query-by-query analysis, it outputs two complete `SELECT`
--   statements to the client NOTICE messages, which can be copied and run.
--
-- Arguments:
--   p_test_set_name (VARCHAR): The name of the test set to compare runs from.
--   p_run_name_1 (VARCHAR): The name of the first run. Use '--original--' to
--     compare against the baseline.
--   p_run_name_2 (VARCHAR): The name of the second run.
--
-- Returns:
--   - Two SELECT statements (via RAISE INFO) for detailed query analysis.
--   - A temporary table `tmp_aggregate_comparison` with a 3-row summary
--     showing totals for each run and the difference between them.
-- =============================================================================
DROP PROCEDURE IF EXISTS perf_framework.compare_runs(varchar, varchar, varchar);
CREATE OR REPLACE PROCEDURE perf_framework.compare_runs(p_test_set_name character varying, p_run_name_1 character varying, p_run_name_2 character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $proc$
DECLARE
    v_test_set_id BIGINT;
    v_run_id_1 BIGINT;
    v_run_id_2 BIGINT;
    v_query_1_sql VARCHAR(1000);
    v_query_2_sql VARCHAR(1000);
BEGIN
    SET application_name = 'perf_framework';
    -- Find the Test Set ID from the original run by its name
    EXECUTE 'SELECT MAX(h.test_set_id) FROM perf_framework.query_history h WHERE h.tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND h.is_original_run = TRUE'
    INTO v_test_set_id;
    IF v_test_set_id IS NULL THEN
        RAISE EXCEPTION '-- Test set with name "%" not found.', p_test_set_name;
    END IF;

    -- Build query for the first run
    IF p_run_name_1 = '--original--' THEN
        v_run_id_1 := 0;
        v_query_1_sql := 'SELECT * FROM perf_framework.v_query_history_consolidated WHERE tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND is_original_run = TRUE ORDER BY submit_time;';
    ELSE
        EXECUTE 'SELECT MAX(h.test_run_id) FROM perf_framework.query_history h WHERE h.test_set_id = ' || v_test_set_id || ' AND h.tags LIKE ''%"test_run_name": "' || p_run_name_1 || '"%'''
        INTO v_run_id_1;
        IF v_run_id_1 IS NULL THEN
            RAISE EXCEPTION '-- Run 1 with name "%" not found in test set "%".', p_run_name_1, p_test_set_name;
        END IF;
        v_query_1_sql := 'SELECT * FROM perf_framework.v_query_history_consolidated WHERE tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND tags LIKE ''%"test_run_name": "' || p_run_name_1 || '"%'' ORDER BY submit_time;';
    END IF;

    -- Build query for the second run
    IF p_run_name_2 = '--original--' THEN
        v_run_id_2 := 0;
        v_query_2_sql := 'SELECT * FROM perf_framework.v_query_history_consolidated WHERE tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND is_original_run = TRUE ORDER BY submit_time;';
    ELSE
        EXECUTE 'SELECT MAX(h.test_run_id) FROM perf_framework.query_history h WHERE h.test_set_id = ' || v_test_set_id || ' AND h.tags LIKE ''%"test_run_name": "' || p_run_name_2 || '"%'''
        INTO v_run_id_2;
        IF v_run_id_2 IS NULL THEN
            RAISE EXCEPTION '-- Run 2 with name "%" not found in test set "%".', p_run_name_2, p_test_set_name;
        END IF;
        v_query_2_sql := 'SELECT * FROM perf_framework.v_query_history_consolidated WHERE tags LIKE ''%"test_set_name": "' || p_test_set_name || '"%'' AND tags LIKE ''%"test_run_name": "' || p_run_name_2 || '"%'' ORDER BY submit_time;';
    END IF;

    -- Create a temporary table for the aggregate comparison
    DROP TABLE IF EXISTS tmp_aggregate_comparison;
    CREATE TEMP TABLE tmp_aggregate_comparison (
        description VARCHAR(255),
        queries_run BIGINT,
        queries_failed BIGINT,
        total_ms NUMERIC(38, 3), run_ms NUMERIC(38, 3), parse_ms NUMERIC(38, 3), wait_parse_ms NUMERIC(38, 3),
        wait_lock_ms NUMERIC(38, 3), plan_ms NUMERIC(38, 3), wait_plan_ms NUMERIC(38, 3), assemble_ms NUMERIC(38, 3),
        wait_assemble_ms NUMERIC(38, 3), compile_ms NUMERIC(38, 3), wait_compile_ms NUMERIC(38, 3),
        acquire_resources_ms NUMERIC(38, 3), wait_run_cpu_ms NUMERIC(38, 3), wait_run_io_ms NUMERIC(38, 3),
        wait_run_spool_ms NUMERIC(38, 3), client_ms NUMERIC(38, 3), wait_client_ms NUMERIC(38, 3),
        cancel_ms NUMERIC(38, 3), restart_ms NUMERIC(38, 3), wlm_runtime_ms NUMERIC(38, 3), spool_ms NUMERIC(38, 3),
        rows_inserted BIGINT, rows_deleted BIGINT, rows_returned BIGINT,
        io_read_bytes BIGINT, io_write_bytes BIGINT, io_spill_read_bytes BIGINT, io_spill_write_bytes BIGINT,
        io_network_bytes BIGINT, io_client_read_bytes BIGINT, io_client_write_bytes BIGINT,
        io_spool_write_bytes BIGINT, io_spill_space_bytes BIGINT
    );

    -- Populate the aggregate comparison table
    INSERT INTO tmp_aggregate_comparison
    WITH run_data AS (
        SELECT
            CASE
                WHEN h.test_run_id = v_run_id_1 OR (v_run_id_1 = 0 AND h.is_original_run) THEN 1
                WHEN h.test_run_id = v_run_id_2 OR (v_run_id_2 = 0 AND h.is_original_run) THEN 2
            END as run_num,
            h.error_code,
            COALESCE(h.total_ms, 0) as total_ms, COALESCE(h.run_ms, 0) as run_ms, COALESCE(h.parse_ms, 0) as parse_ms, COALESCE(h.wait_parse_ms, 0) as wait_parse_ms,
            COALESCE(h.wait_lock_ms, 0) as wait_lock_ms, COALESCE(h.plan_ms, 0) as plan_ms, COALESCE(h.wait_plan_ms, 0) as wait_plan_ms, COALESCE(h.assemble_ms, 0) as assemble_ms,
            COALESCE(h.wait_assemble_ms, 0) as wait_assemble_ms, COALESCE(h.compile_ms, 0) as compile_ms, COALESCE(h.wait_compile_ms, 0) as wait_compile_ms,
            COALESCE(h.acquire_resources_ms, 0) as acquire_resources_ms, COALESCE(h.wait_run_cpu_ms, 0) as wait_run_cpu_ms, COALESCE(h.wait_run_io_ms, 0) as wait_run_io_ms,
            COALESCE(h.wait_run_spool_ms, 0) as wait_run_spool_ms, COALESCE(h.client_ms, 0) as client_ms, COALESCE(h.wait_client_ms, 0) as wait_client_ms,
            COALESCE(h.cancel_ms, 0) as cancel_ms, COALESCE(h.restart_ms, 0) as restart_ms, COALESCE(h.wlm_runtime_ms, 0) as wlm_runtime_ms, COALESCE(h.spool_ms, 0) as spool_ms,
            COALESCE(h.rows_inserted, 0) as rows_inserted, COALESCE(h.rows_deleted, 0) as rows_deleted, COALESCE(h.rows_returned, 0) as rows_returned,
            COALESCE(h.io_read_bytes, 0) as io_read_bytes, COALESCE(h.io_write_bytes, 0) as io_write_bytes, COALESCE(h.io_spill_read_bytes, 0) as io_spill_read_bytes,
            COALESCE(h.io_spill_write_bytes, 0) as io_spill_write_bytes, COALESCE(h.io_network_bytes, 0) as io_network_bytes, COALESCE(h.io_client_read_bytes, 0) as io_client_read_bytes,
            COALESCE(h.io_client_write_bytes, 0) as io_client_write_bytes, COALESCE(h.io_spool_write_bytes, 0) as io_spool_write_bytes, COALESCE(h.io_spill_space_bytes, 0) as io_spill_space_bytes
        FROM perf_framework.v_query_history_consolidated h
        WHERE h.test_set_id = v_test_set_id
          AND (
              (h.test_run_id = v_run_id_1 OR h.test_run_id = v_run_id_2)
              OR
              (h.is_original_run = TRUE AND (v_run_id_1 = 0 OR v_run_id_2 = 0))
          )
    ),
    summary AS (
        SELECT
            SUM(DECODE(run_num = 1, TRUE, 1, 0)) AS queries_run_1,
            SUM(DECODE(run_num = 2, TRUE, 1, 0)) AS queries_run_2,
            SUM(DECODE(run_num = 1 AND COALESCE(error_code, 'XXXXX') <> '00000', TRUE, 1, 0)) AS queries_failed_1,
            SUM(DECODE(run_num = 2 AND COALESCE(error_code, 'XXXXX') <> '00000', TRUE, 1, 0)) AS queries_failed_2,
            SUM(CASE WHEN run_num = 1 THEN total_ms END) as total_ms_1, SUM(CASE WHEN run_num = 2 THEN total_ms END) as total_ms_2,
            SUM(CASE WHEN run_num = 1 THEN run_ms END) as run_ms_1, SUM(CASE WHEN run_num = 2 THEN run_ms END) as run_ms_2,
            SUM(CASE WHEN run_num = 1 THEN parse_ms END) as parse_ms_1, SUM(CASE WHEN run_num = 2 THEN parse_ms END) as parse_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_parse_ms END) as wait_parse_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_parse_ms END) as wait_parse_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_lock_ms END) as wait_lock_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_lock_ms END) as wait_lock_ms_2,
            SUM(CASE WHEN run_num = 1 THEN plan_ms END) as plan_ms_1, SUM(CASE WHEN run_num = 2 THEN plan_ms END) as plan_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_plan_ms END) as wait_plan_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_plan_ms END) as wait_plan_ms_2,
            SUM(CASE WHEN run_num = 1 THEN assemble_ms END) as assemble_ms_1, SUM(CASE WHEN run_num = 2 THEN assemble_ms END) as assemble_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_assemble_ms END) as wait_assemble_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_assemble_ms END) as wait_assemble_ms_2,
            SUM(CASE WHEN run_num = 1 THEN compile_ms END) as compile_ms_1, SUM(CASE WHEN run_num = 2 THEN compile_ms END) as compile_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_compile_ms END) as wait_compile_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_compile_ms END) as wait_compile_ms_2,
            SUM(CASE WHEN run_num = 1 THEN acquire_resources_ms END) as acquire_resources_ms_1, SUM(CASE WHEN run_num = 2 THEN acquire_resources_ms END) as acquire_resources_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_run_cpu_ms END) as wait_run_cpu_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_run_cpu_ms END) as wait_run_cpu_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_run_io_ms END) as wait_run_io_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_run_io_ms END) as wait_run_io_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_run_spool_ms END) as wait_run_spool_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_run_spool_ms END) as wait_run_spool_ms_2,
            SUM(CASE WHEN run_num = 1 THEN client_ms END) as client_ms_1, SUM(CASE WHEN run_num = 2 THEN client_ms END) as client_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wait_client_ms END) as wait_client_ms_1, SUM(CASE WHEN run_num = 2 THEN wait_client_ms END) as wait_client_ms_2,
            SUM(CASE WHEN run_num = 1 THEN cancel_ms END) as cancel_ms_1, SUM(CASE WHEN run_num = 2 THEN cancel_ms END) as cancel_ms_2,
            SUM(CASE WHEN run_num = 1 THEN restart_ms END) as restart_ms_1, SUM(CASE WHEN run_num = 2 THEN restart_ms END) as restart_ms_2,
            SUM(CASE WHEN run_num = 1 THEN wlm_runtime_ms END) as wlm_runtime_ms_1, SUM(CASE WHEN run_num = 2 THEN wlm_runtime_ms END) as wlm_runtime_ms_2,
            SUM(CASE WHEN run_num = 1 THEN spool_ms END) as spool_ms_1, SUM(CASE WHEN run_num = 2 THEN spool_ms END) as spool_ms_2,
            SUM(CASE WHEN run_num = 1 THEN rows_inserted END) as rows_inserted_1, SUM(CASE WHEN run_num = 2 THEN rows_inserted END) as rows_inserted_2,
            SUM(CASE WHEN run_num = 1 THEN rows_deleted END) as rows_deleted_1, SUM(CASE WHEN run_num = 2 THEN rows_deleted END) as rows_deleted_2,
            SUM(CASE WHEN run_num = 1 THEN rows_returned END) as rows_returned_1, SUM(CASE WHEN run_num = 2 THEN rows_returned END) as rows_returned_2,
            SUM(CASE WHEN run_num = 1 THEN io_read_bytes END) as io_read_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_read_bytes END) as io_read_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_write_bytes END) as io_write_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_write_bytes END) as io_write_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_spill_read_bytes END) as io_spill_read_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_spill_read_bytes END) as io_spill_read_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_spill_write_bytes END) as io_spill_write_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_spill_write_bytes END) as io_spill_write_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_network_bytes END) as io_network_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_network_bytes END) as io_network_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_client_read_bytes END) as io_client_read_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_client_read_bytes END) as io_client_read_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_client_write_bytes END) as io_client_write_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_client_write_bytes END) as io_client_write_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_spool_write_bytes END) as io_spool_write_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_spool_write_bytes END) as io_spool_write_bytes_2,
            SUM(CASE WHEN run_num = 1 THEN io_spill_space_bytes END) as io_spill_space_bytes_1, SUM(CASE WHEN run_num = 2 THEN io_spill_space_bytes END) as io_spill_space_bytes_2
        FROM run_data
    )
    SELECT
        description,
        queries_run, queries_failed,
        total_ms, run_ms, parse_ms, wait_parse_ms, wait_lock_ms, plan_ms, wait_plan_ms, assemble_ms, wait_assemble_ms,
        compile_ms, wait_compile_ms, acquire_resources_ms, wait_run_cpu_ms, wait_run_io_ms, wait_run_spool_ms,
        client_ms, wait_client_ms, cancel_ms, restart_ms, wlm_runtime_ms, spool_ms,
        rows_inserted, rows_deleted, rows_returned,
        io_read_bytes, io_write_bytes, io_spill_read_bytes, io_spill_write_bytes,
        io_network_bytes, io_client_read_bytes, io_client_write_bytes,
        io_spool_write_bytes, io_spill_space_bytes
    FROM (
        SELECT 1 as sort_order, p_run_name_1 as description, queries_run_1, queries_failed_1, total_ms_1, run_ms_1, parse_ms_1, wait_parse_ms_1, wait_lock_ms_1, plan_ms_1, wait_plan_ms_1, assemble_ms_1, wait_assemble_ms_1, compile_ms_1, wait_compile_ms_1, acquire_resources_ms_1, wait_run_cpu_ms_1, wait_run_io_ms_1, wait_run_spool_ms_1, client_ms_1, wait_client_ms_1, cancel_ms_1, restart_ms_1, wlm_runtime_ms_1, spool_ms_1, rows_inserted_1, rows_deleted_1, rows_returned_1, io_read_bytes_1, io_write_bytes_1, io_spill_read_bytes_1, io_spill_write_bytes_1, io_network_bytes_1, io_client_read_bytes_1, io_client_write_bytes_1, io_spool_write_bytes_1, io_spill_space_bytes_1 FROM summary
        UNION ALL
        SELECT 2 as sort_order, p_run_name_2, queries_run_2, queries_failed_2, total_ms_2, run_ms_2, parse_ms_2, wait_parse_ms_2, wait_lock_ms_2, plan_ms_2, wait_plan_ms_2, assemble_ms_2, wait_assemble_ms_2, compile_ms_2, wait_compile_ms_2, acquire_resources_ms_2, wait_run_cpu_ms_2, wait_run_io_ms_2, wait_run_spool_ms_2, client_ms_2, wait_client_ms_2, cancel_ms_2, restart_ms_2, wlm_runtime_ms_2, spool_ms_2, rows_inserted_2, rows_deleted_2, rows_returned_2, io_read_bytes_2, io_write_bytes_2, io_spill_read_bytes_2, io_spill_write_bytes_2, io_network_bytes_2, io_client_read_bytes_2, io_client_write_bytes_2, io_spool_write_bytes_2, io_spill_space_bytes_2 FROM summary
        UNION ALL
        SELECT 3 as sort_order, 'Difference', queries_run_2 - queries_run_1, queries_failed_2 - queries_failed_1, total_ms_2 - total_ms_1, run_ms_2 - run_ms_1, parse_ms_2 - parse_ms_1, wait_parse_ms_2 - wait_parse_ms_1, wait_lock_ms_2 - wait_lock_ms_1, plan_ms_2 - plan_ms_1, wait_plan_ms_2 - wait_plan_ms_1, assemble_ms_2 - assemble_ms_1, wait_assemble_ms_2 - wait_assemble_ms_1, compile_ms_2 - compile_ms_1, wait_compile_ms_2 - wait_compile_ms_1, acquire_resources_ms_2 - acquire_resources_ms_1, wait_run_cpu_ms_2 - wait_run_cpu_ms_1, wait_run_io_ms_2 - wait_run_io_ms_1, wait_run_spool_ms_2 - wait_run_spool_ms_1, client_ms_2 - client_ms_1, wait_client_ms_2 - wait_client_ms_1, cancel_ms_2 - cancel_ms_1, restart_ms_2 - restart_ms_1, wlm_runtime_ms_2 - wlm_runtime_ms_1, spool_ms_2 - spool_ms_1, rows_inserted_2 - rows_inserted_1, rows_deleted_2 - rows_deleted_1, rows_returned_2 - rows_returned_1, io_read_bytes_2 - io_read_bytes_1, io_write_bytes_2 - io_write_bytes_1, io_spill_read_bytes_2 - io_spill_read_bytes_1, io_spill_write_bytes_2 - io_spill_write_bytes_1, io_network_bytes_2 - io_network_bytes_1, io_client_read_bytes_2 - io_client_read_bytes_1, io_client_write_bytes_2 - io_client_write_bytes_1, io_spool_write_bytes_2 - io_spool_write_bytes_1, io_spill_space_bytes_2 - io_spill_space_bytes_1 FROM summary
    ) AS final_summary(sort_order, description, queries_run, queries_failed, total_ms, run_ms, parse_ms, wait_parse_ms, wait_lock_ms, plan_ms, wait_plan_ms, assemble_ms, wait_assemble_ms, compile_ms, wait_compile_ms, acquire_resources_ms, wait_run_cpu_ms, wait_run_io_ms, wait_run_spool_ms, client_ms, wait_client_ms, cancel_ms, restart_ms, wlm_runtime_ms, spool_ms, rows_inserted, rows_deleted, rows_returned, io_read_bytes, io_write_bytes, io_spill_read_bytes, io_spill_write_bytes, io_network_bytes, io_client_read_bytes, io_client_write_bytes, io_spool_write_bytes, io_spill_space_bytes)
    ORDER BY sort_order;

    RAISE INFO '--Comparison complete.';
    RAISE INFO '--';
    RAISE INFO '-- Query for detailed results from Run 1 (''%''). Copy and execute the line below:', p_run_name_1;
    RAISE INFO '%', v_query_1_sql;
    RAISE INFO '';
    RAISE INFO '--';
    RAISE INFO '-- Query for detailed results from Run 2 (''%''). Copy and execute the line below:', p_run_name_2;
    RAISE INFO '%', v_query_2_sql;
    RAISE INFO '';
    RAISE INFO '--';
    RAISE INFO '-- Query aggregate summary results';
    RAISE INFO 'SELECT * FROM tmp_aggregate_comparison;';
END;
$proc$
;

-- Table types for set-returning procedures
DROP TABLE IF EXISTS perf_framework.test_set_name_type;
CREATE TABLE perf_framework.test_set_name_type (test_set_name VARCHAR);
DROP TABLE IF EXISTS perf_framework.test_run_name_type;
CREATE TABLE perf_framework.test_run_name_type (test_set_name VARCHAR, run_name VARCHAR);

-- Procedure: show_test_sets (returns SETOF test_set_name_type)
DROP PROCEDURE IF EXISTS perf_framework.show_test_sets();
CREATE OR REPLACE PROCEDURE perf_framework.show_test_sets()
RETURNS SETOF perf_framework.test_set_name_type
LANGUAGE plpgsql
AS $$
DECLARE
    rec perf_framework.test_set_name_type%ROWTYPE;
BEGIN
    SET application_name = 'perf_framework';
    FOR rec IN
        EXECUTE 'SELECT DISTINCT (regexp_replace(tags, ''.*"test_set_name": "([^"]+)".*'', ''\1''))::VARCHAR AS test_set_name FROM perf_framework.query_history WHERE tags LIKE ''%"test_set_name":%'' ORDER BY 1'
    LOOP
        RETURN NEXT rec;
    END LOOP;
END;
$$;

-- Procedure: show_test_runs (returns SETOF test_run_name_type)
DROP PROCEDURE IF EXISTS perf_framework.show_test_runs();
CREATE OR REPLACE PROCEDURE perf_framework.show_test_runs()
RETURNS SETOF perf_framework.test_run_name_type
LANGUAGE plpgsql
AS $$
DECLARE
    rec perf_framework.test_run_name_type%ROWTYPE;
BEGIN
    SET application_name = 'perf_framework';
    FOR rec IN
        EXECUTE 'SELECT DISTINCT (regexp_replace(tags, ''.*"test_set_name": "([^"]+)".*'', ''\1''))::VARCHAR AS test_set_name, (regexp_replace(tags, ''.*"test_run_name": "([^"]+)".*'', ''\1''))::VARCHAR AS run_name FROM perf_framework.query_history WHERE tags LIKE ''%"test_set_name":%'' AND tags LIKE ''%"test_run_name":%'''
    LOOP
        RETURN NEXT rec;
    END LOOP;
END;
$$;

-- Utility procedure: wait_and_archive_test_run
-- Waits for all queries to complete and then archives the test run
DROP PROCEDURE IF EXISTS perf_framework.wait_and_archive_test_run(VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE perf_framework.wait_and_archive_test_run(
    p_test_set_name VARCHAR,
    p_test_run_name VARCHAR
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_test_set_id BIGINT;
    v_test_run_id BIGINT;
    v_remaining BIGINT;
    v_total BIGINT;
BEGIN
    SET application_name = 'perf_framework';
    -- Look up IDs from names
    SELECT MAX(test_set_id) INTO v_test_set_id
    FROM perf_framework.query_history
    WHERE tags LIKE '%"test_set_name": "' || p_test_set_name || '"%' AND is_original_run = TRUE;
    IF v_test_set_id IS NULL THEN
        RAISE EXCEPTION '-- Test set with name "%" not found.', p_test_set_name;
    END IF;
    -- Lookup test_run_id from work_queue instead of query_history
    SELECT MAX(test_run_id) INTO v_test_run_id
    FROM perf_framework.work_queue
    WHERE test_set_name = p_test_set_name AND test_run_name = p_test_run_name;
    IF v_test_run_id IS NULL THEN
        RAISE EXCEPTION '-- Test run with name "%" not found for test set "%".', p_test_run_name, p_test_set_name;
    END IF;
    SELECT COUNT(*) INTO v_total FROM perf_framework.work_queue WHERE test_run_id = v_test_run_id;
    LOOP
        SELECT COUNT(*) INTO v_remaining
        FROM perf_framework.work_queue
        WHERE test_run_id = v_test_run_id AND status <> 'complete';
        IF v_remaining = 0 THEN
            RAISE INFO '-- All queries complete. Archiving results...';
            EXECUTE 'CALL perf_framework._internal_wait_for_logs(''' || v_test_run_id || ''', ' || v_total || ')';
            EXECUTE 'CALL perf_framework.archive_test_run(' || v_test_set_id || ', ' || v_test_run_id || ')';
            EXIT;
        ELSE
            RAISE INFO '-- % queries remaining to complete...', v_remaining;
            PERFORM sys.sleep(2000);
        END IF;
    END LOOP;
END;
$proc$;