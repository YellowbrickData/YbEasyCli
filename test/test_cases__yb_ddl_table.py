test_cases = [
    test_case(
        cmd='yb_ddl_table.py @{argsdir}/db1 --current_schema dev --table_like a1_t --'
        , exit_code=0
        , stdout="""CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev '"Prod"' --table_like a1_t --""")
        , exit_code=0
        , stdout="""CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev '"Prod"' --with_schema --table_like a1_t --""")
        , exit_code=0
        , stdout="""CREATE TABLE dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE "Prod".a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev '"Prod"' --with_db --table_like a1_t --""")
        , exit_code=0
        , stdout="""CREATE TABLE {db1}.dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE {db1}."Prod".a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')
]