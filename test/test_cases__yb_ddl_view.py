test_cases = [
    test_case(
        cmd='yb_ddl_view.py @{argsdir}/db1 --current_schema dev --view_like a1_v --'
        , exit_code=0
        , stdout="""CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM a1_t;"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_ddl_view.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev '"Prod"' --view_like a1_v --""")
        , exit_code=0
        , stdout="""
CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM a1_t;

CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM "Prod".a1_t;"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_ddl_view.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev '"Prod"' --with_schema --view_like a1_v --""")
        , exit_code=0
        , stdout="""CREATE VIEW dev.a1_v AS
 SELECT a1_t.col1
   FROM a1_t;

CREATE VIEW "Prod".a1_v AS
 SELECT a1_t.col1
   FROM "Prod".a1_t;"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_ddl_view.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev '"Prod"' --with_db --view_like a1_v --""")
        , exit_code=0
        , stdout="""CREATE VIEW {db1}.dev.a1_v AS
 SELECT a1_t.col1
   FROM a1_t;

CREATE VIEW {db1}."Prod".a1_v AS
 SELECT a1_t.col1
   FROM "Prod".a1_t;"""
        , stderr='')
]