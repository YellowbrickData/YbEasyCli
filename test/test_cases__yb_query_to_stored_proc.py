test_cases = [
    test_case(
        cmd="yb_query_to_stored_proc.py @{argsdir}/db1 --stored_proc dev.log_query_p --query 'SELECT * FROM sys.log_query'"
        , exit_code=0
        , stdout="""-- Creating the dev.log_query_p stored procedure for the query provided.
-- Created."""
        , stderr='')

    , test_case(
        cmd='yb_query_to_stored_proc.py @{argsdir}/db1 --stored_proc dev.log_query_p --drop'
        , exit_code=0
        , stdout="""-- Dropping the dev.log_query_p stored procedure.
-- Dropped."""
        , stderr='')
]