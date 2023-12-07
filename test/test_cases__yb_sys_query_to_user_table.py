test_cases = [
    test_case(
        cmd="""yb_sys_query_to_user_table.py \
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
' --create_table --as_temp_table"""
        , exit_code=0
        , stdout=("""-- Converting system query to user table.
Prod
dev
public
sys
-- The schema user table has been created."""
            if self.ybdb_version_major < 5 else """-- Converting system query to user table.
Prod
dev
information_schema
public
sys
-- The schema user table has been created.""")
        , stderr='')

    , test_case(
        cmd=
            """yb_sys_query_to_user_table.py @{argsdir}/db1 --query 'SELECT * FROM dev.data_types_t' --table 'schema' --create_table"""
        , exit_code=(0 if Common.is_windows else 1)
        , stdout="""-- Converting system query to user table."""
        , stderr='yb_sys_query_to_user_table.py: ERROR:  table/s from column store database are NOT permitted in the input query...')
]