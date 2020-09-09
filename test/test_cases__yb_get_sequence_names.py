test_cases = [
    test_case(cmd='yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev '
            '--schema_in dev --'
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev '"Prod"' --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_like '%%1%%' --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_like '%%1%%' --sequence_NOTlike '%%c%%' --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_like '%%1%%' --sequence_NOTin b1_seq --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.c1_seq
"Prod".a1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_in a1_seq c1_seq --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.c1_seq
"Prod".a1_seq"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_in a1_seq c1_seq --sequence_like '%%1%%' --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_in a1_seq c1_seq --sequence_like '%%1%%' --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_in a1_seq c1_seq --sequence_like '%%1%%' --owner_in {user_name} --""")
        , exit_code=0
        , stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
"Prod".a1_seq
"Prod".b1_seq
"Prod"."C1_seq" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_sequence_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --sequence_in a1_seq c1_seq data_types_seq --sequence_like '%%1%%' """
            "--owner_in no_such_user --")
        , exit_code=0
        , stdout=''
        , stderr='')
]