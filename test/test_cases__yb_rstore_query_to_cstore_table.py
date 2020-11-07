test_cases = [
    test_case(
        cmd="""yb_rstore_query_to_cstore_table.py \
@{argsdir}/db1 --query '\
SELECT         \
    name       \
FROM           \
    sys.schema \
' --table 'schema' --post_sql '\
SELECT     \
    *      \
FROM       \
    schema \
ORDER BY   \
    1      \
' --create_temp_table"""
        , exit_code=0
        , stdout="""-- Converting row store query to column store table.
Prod
dev
public
sys
-- The schema column store table has been created."""
        , stderr='')

    , test_case(
        cmd=
            """yb_rstore_query_to_cstore_table.py @{argsdir}/db1 --query 'SELECT * FROM dev.data_types_t' --table 'schema'"""
        , exit_code=1
        , stdout="""-- Converting row store query to column store table."""
        , stderr='yb_rstore_query_to_cstore_table.py: ERROR:  table/s from column store databese are NOT permitted in the input query...')
]