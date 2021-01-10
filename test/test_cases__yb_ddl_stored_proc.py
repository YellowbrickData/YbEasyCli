test_cases = [
    test_case(cmd='yb_ddl_stored_proc.py @{argsdir}/db1 --with_schema --schema_in dev --stored_proc_in test_error_p'
        , exit_code=0
        , stdout="""CREATE PROCEDURE dev.test_error_p(dummy_test_arg_1 bigint DEFAULT 1)
 RETURNS void
 LANGUAGE plpgsql
AS $CODE$
DECLARE
    v_rec RECORD;
    v_state TEXT;
    v_msg TEXT;
    v_detail TEXT;
    v_hint TEXT;
    v_context TEXT;
BEGIN
    EXECUTE 'SELECT * FROM "Does Not Exist"' INTO v_rec;
EXCEPTION
    WHEN SQLSTATE '42P01' THEN
        GET STACKED DIAGNOSTICS
            v_state   = returned_sqlstate,
            v_msg     = message_text,
            v_detail  = pg_exception_detail,
            v_hint    = pg_exception_hint,
            v_context = pg_exception_context;
        RAISE INFO 'SQLERRM ====> %', SQLERRM;
        RAISE INFO 'SQLSTATE ===> %', SQLSTATE;
        RAISE INFO 'v_state ====> %', v_state;
        RAISE INFO 'v_msg ======> %', v_msg;
        RAISE INFO 'v_detail ===> %', v_detail;
        RAISE INFO 'v_hint =====> %', v_hint;
        RAISE INFO 'v_context ==> %', v_context;
    WHEN OTHERS THEN
        NULL;
END;$CODE$"""
        , stderr='')

    , test_case(cmd='yb_ddl_stored_proc.py @{argsdir}/db1 --with_db --schema_in Prod --stored_proc_in test_error_p'
        , exit_code=0
        , stdout="""CREATE PROCEDURE {db1}."Prod".test_error_p(dummy_test_arg_1 bigint DEFAULT 1)
 RETURNS void
 LANGUAGE plpgsql
AS $CODE$
DECLARE
    v_rec RECORD;
    v_state TEXT;
    v_msg TEXT;
    v_detail TEXT;
    v_hint TEXT;
    v_context TEXT;
BEGIN
    EXECUTE 'SELECT * FROM "Does Not Exist"' INTO v_rec;
EXCEPTION
    WHEN SQLSTATE '42P01' THEN
        GET STACKED DIAGNOSTICS
            v_state   = returned_sqlstate,
            v_msg     = message_text,
            v_detail  = pg_exception_detail,
            v_hint    = pg_exception_hint,
            v_context = pg_exception_context;
        RAISE INFO 'SQLERRM ====> %', SQLERRM;
        RAISE INFO 'SQLSTATE ===> %', SQLSTATE;
        RAISE INFO 'v_state ====> %', v_state;
        RAISE INFO 'v_msg ======> %', v_msg;
        RAISE INFO 'v_detail ===> %', v_detail;
        RAISE INFO 'v_hint =====> %', v_hint;
        RAISE INFO 'v_context ==> %', v_context;
    WHEN OTHERS THEN
        NULL;
END;$CODE$"""
        , stderr='')

    , test_case(cmd='yb_ddl_stored_proc.py @{argsdir}/db1 --schema_in Prod'
        , exit_code=0
        , stdout="""CREATE PROCEDURE query_definer_p(bigint DEFAULT 1, numeric DEFAULT 1)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $CODE$
DECLARE
    v_rec RECORD;
BEGIN
    FOR v_rec IN SELECT * FROM sys.query
    LOOP
        RAISE INFO '%', v_rec;
    END LOOP;
END;$CODE$

CREATE PROCEDURE test_error_p(dummy_test_arg_1 bigint DEFAULT 1)
 RETURNS void
 LANGUAGE plpgsql
AS $CODE$
DECLARE
    v_rec RECORD;
    v_state TEXT;
    v_msg TEXT;
    v_detail TEXT;
    v_hint TEXT;
    v_context TEXT;
BEGIN
    EXECUTE 'SELECT * FROM "Does Not Exist"' INTO v_rec;
EXCEPTION
    WHEN SQLSTATE '42P01' THEN
        GET STACKED DIAGNOSTICS
            v_state   = returned_sqlstate,
            v_msg     = message_text,
            v_detail  = pg_exception_detail,
            v_hint    = pg_exception_hint,
            v_context = pg_exception_context;
        RAISE INFO 'SQLERRM ====> %', SQLERRM;
        RAISE INFO 'SQLSTATE ===> %', SQLSTATE;
        RAISE INFO 'v_state ====> %', v_state;
        RAISE INFO 'v_msg ======> %', v_msg;
        RAISE INFO 'v_detail ===> %', v_detail;
        RAISE INFO 'v_hint =====> %', v_hint;
        RAISE INFO 'v_context ==> %', v_context;
    WHEN OTHERS THEN
        NULL;
END;$CODE$

CREATE PROCEDURE "test_Raise_p"(dummy_test_arg_1 bigint DEFAULT 1, dummy_test_arg_2 character varying DEFAULT 'xxxx'::character varying(4))
 RETURNS INTEGER
 LANGUAGE plpgsql
 SET client_min_messages TO 'notice'
AS $CODE$
BEGIN
    RAISE INFO 'Test RAISE INFO, is always displayed regardless of client_min_message!';
    RAISE INFO '';
    --
    RAISE DEBUG 'Test RAISE DEBUG!';
    RAISE LOG 'Test RAISE LOG!';
    RAISE NOTICE 'Test RAISE NOTICE!';
    RAISE WARNING 'Test RAISE WARNING!';
    RAISE EXCEPTION 'Test RAISE EXCEPTION!';
    RETURN -1;
END;$CODE$"""
        , stderr='')
]