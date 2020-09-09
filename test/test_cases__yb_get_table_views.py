test_cases = [
    test_case(cmd='yb_get_view_names.py @{argsdir}/db1 --current_schema dev'
        , exit_code=0
        , stdout="""a1_v
b1_v
c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_view_names.py @{argsdir}/db1 --current_schema dev '
            '--schemas dev prod')
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
dev.c1_v
prod.a1_v
prod.b1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --like '%%1%%'")
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
dev.c1_v
prod.a1_v
prod.b1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --like '%%1%%' --NOTlike '%%c%%'")
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
prod.a1_v
prod.b1_v""",
        stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --like '%%1%%' --NOTin b1_v")
        , exit_code=0
        , stdout="""dev.a1_v
dev.c1_v
prod.a1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --in a1_v c1_v")
        , exit_code=0
        , stdout="""dev.a1_v
dev.c1_v
prod.a1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --in a1_v c1_v --like '%%1%%'")
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
dev.c1_v
prod.a1_v
prod.b1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --in a1_v c1_v --like '%%1%%'")
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
dev.c1_v
prod.a1_v
prod.b1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
            "--schemas dev prod --in a1_v c1_v --like '%%1%%' --owner {user_name}")
        , exit_code=0
        , stdout="""dev.a1_v
dev.b1_v
dev.c1_v
prod.a1_v
prod.b1_v
prod.c1_v"""
        , stderr='')

    , test_case(
        cmd=("yb_get_view_names.py @{argsdir}/db1 --current_schema dev "
             "--schemas dev prod --in a1_v c1_v data_vypes_v --like '%%1%%' "
             "--owner no_such_user")
        , exit_code=0
        , stdout=''
        , stderr='')
]