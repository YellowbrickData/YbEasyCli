test_cases = [
    test_case(
        cmd='yb_ddl_sequence.py @{argsdir}/db1 --schema_in dev --sequence_like a1_seq'
        , exit_code=3
        , stdout=""
        , stderr="""yb_ddl_sequence.py: ERROR:  relation "a1_seq" does not exist
LINE 1: ..._value END AS min_value, cache_value, is_cycled FROM a1_seq;
                                                                ^
QUERY:  SELECT sequence_name, start_value, increment_by, CASE WHEN increment_by > 0 AND max_value = 9223372036854775807 THEN NULL      WHEN increment_by < 0 AND max_value = -1 THEN NULL      ELSE max_value END AS max_value, CASE WHEN increment_by > 0 AND min_value = 1 THEN NULL      WHEN increment_by < 0 AND min_value = -9223372036854775807 THEN NULL      ELSE min_value END AS min_value, cache_value, is_cycled FROM a1_seq;"""
        , comment="bug: YBD-18137")

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev Prod --sequence_like a1_seq""")
        , exit_code=(3 if self.ybdb_version_major < 5 else 0)
        , stdout=("""CREATE SEQUENCE a1_seq START WITH 1000448;"""
            if self.ybdb_version_major < 5
            else """CREATE SEQUENCE a1_seq START WITH 1000448;

CREATE SEQUENCE a1_seq START WITH 1000448;""")
        , stderr="""yb_ddl_sequence.py: ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;"""
        if self.ybdb_version_major < 5
        else ''
        , comment='waiting YBD-16762 fix.')

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev --schema_in '
            """dev Prod --with_schema --sequence_like a1_seq""")
        , exit_code=(3 if self.ybdb_version_major < 5 else 0)
        , stdout="""CREATE SEQUENCE dev.a1_seq START WITH 1000448;"""
        if self.ybdb_version_major < 5
        else """CREATE SEQUENCE dev.a1_seq START WITH 1000448;

CREATE SEQUENCE "Prod".a1_seq START WITH 1000448;"""
        , stderr="""yb_ddl_sequence.py: ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;"""
        if self.ybdb_version_major < 5
        else '')

    , test_case(
        cmd=(
            'yb_ddl_sequence.py @{argsdir}/db1 --current_schema dev  --schema_in '
            """dev Prod --with_db --sequence_like a1_seq""")
        , exit_code=(3 if self.ybdb_version_major < 5 else 0)
        , stdout="""CREATE SEQUENCE {db1}.dev.a1_seq START WITH 1000448;"""
        if self.ybdb_version_major < 5
        else """CREATE SEQUENCE dze_db1.dev.a1_seq START WITH 1000448;

CREATE SEQUENCE dze_db1."Prod".a1_seq START WITH 1000448;"""
        , stderr="""yb_ddl_sequence.py: ERROR:  relation "prod.a1_seq" does not exist
LINE 1: SELECT * FROM Prod.a1_seq;
                      ^
QUERY:  SELECT * FROM Prod.a1_seq;"""
        if self.ybdb_version_major < 5
        else '')
]