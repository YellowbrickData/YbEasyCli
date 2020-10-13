test_cases = [
    test_case(
        cmd='yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev --sequence_like a1_seq --'
        , exit_code=0
        , stdout="""CREATE SEQUENCE a1_seq START WITH 1000448;"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev Prod --sequence_like a1_seq --""")
        , exit_code=3
        , stdout="""CREATE SEQUENCE a1_seq START WITH 1000448;"""
        , stderr="""ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;"""
        , comment='waiting YBD-16762 fix.')

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev Prod --with_schema --sequence_like a1_seq --""")
        , exit_code=3
        , stdout="""CREATE SEQUENCE dev.a1_seq START WITH 1000448;"""
        , stderr="""ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;""")

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev  --schema_in '
            """dev Prod --with_db --sequence_like a1_seq --""")
        , exit_code=3
        , stdout="""CREATE SEQUENCE {db1}.dev.a1_seq START WITH 1000448;"""
        , stderr="""ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;""")
]