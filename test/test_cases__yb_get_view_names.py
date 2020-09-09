test_cases = [
    test_case(
        cmd='yb_get_view_names.py @{argsdir}/db1 --schema_in dev --current_schema dev --'
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_view_names.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev '"Prod"' --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_like '%%1%%' --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_like '%%1%%' --view_NOTlike '%%c%%' --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_like '%%1%%' --view_NOTin b1_v --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_in a1_v c1_v --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_in a1_v c1_v --view_like '%%1%%' --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_in a1_v c1_v --view_like '%%1%%' --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_in a1_v c1_v --view_like '%%1%%' --owner_in {user_name} --""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_v
{db1}.dev.b1_v
{db1}.dev.c1_v
{db1}."Prod".a1_v
{db1}."Prod".b1_v
{db1}."Prod"."C1_v" """
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            """--schema_in dev '"Prod"' --view_in a1_v c1_v data_vypes_v --view_like '%%1%%' """
            "--owner_in no_such_user --")
        , exit_code=0
        , stdout=''
        , stderr='')
]