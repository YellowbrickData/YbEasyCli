test_cases = [
    test_case(cmd='yb_get_table_names.py @{argsdir}/db1 --schema_in dev'
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}.dev.c1_t
{db1}.dev.data_types_t
{db1}.dev.dist_random_t
{db1}.dev.dist_replicate_t"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_names.py @{argsdir}/db1'
            """ --schema_in dev Prod""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}.dev.c1_t
{db1}.dev.data_types_t
{db1}.dev.dist_random_t
{db1}.dev.dist_replicate_t
{db1}."Prod".a1_t
{db1}."Prod".b1_t
{db1}."Prod"."C1_t"
{db1}."Prod".data_types_t"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_like '%%1_%%'""")
        , exit_code=0
        , stdout=("""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}.dev.c1_t
{db1}."Prod".a1_t
{db1}."Prod".b1_t
{db1}."Prod"."C1_t""" + '"')
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_like '%%1_%%' --table_NOTlike '%%c%%' '%%C%%'""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}."Prod".a1_t
{db1}."Prod".b1_t"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_like '%%1_%%' --table_NOTin b1_t""")
        , exit_code=0
        , stdout=("""{db1}.dev.a1_t
{db1}.dev.c1_t
{db1}."Prod".a1_t
{db1}."Prod"."C1_t""" + '"')
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_in a1_t c1_t data_types_t""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.c1_t
{db1}.dev.data_types_t
{db1}."Prod".a1_t
{db1}."Prod".data_types_t"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_in a1_t c1_t data_types_t --table_like '%%1_%%'""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}.dev.c1_t
{db1}.dev.data_types_t
{db1}."Prod".a1_t
{db1}."Prod".b1_t
{db1}."Prod"."C1_t"
{db1}."Prod".data_types_t"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1"
            """ --schema_in dev Prod --table_in a1_t c1_t data_types_t --table_like '%%1_%%' --owner_in {user_name}""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t
{db1}.dev.b1_t
{db1}.dev.c1_t
{db1}.dev.data_types_t
{db1}."Prod".a1_t
{db1}."Prod".b1_t
{db1}."Prod"."C1_t"
{db1}."Prod".data_types_t"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1 --current_schema dev"
            """ --schema_in dev Prod --table_in a1_t c1_t data_types_t --table_like '%%1%%' --owner_in no_such_user""")
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_table_names.py @{argsdir}/db1 --schema_in dev Prod"
            """ --output_template "SELECT '{{table_path}} rows: ' || COUNT(*) FROM {{table_path}};" --exec_output""")
        , exit_code=0
        , stdout="""{db1}.dev.a1_t rows: 0
{db1}.dev.b1_t rows: 0
{db1}.dev.c1_t rows: 0
{db1}.dev.data_types_t rows: 1000000
{db1}.dev.dist_random_t rows: 0
{db1}.dev.dist_replicate_t rows: 0
{db1}."Prod".a1_t rows: 0
{db1}."Prod".b1_t rows: 0
{db1}."Prod"."C1_t" rows: 0
{db1}."Prod".data_types_t rows: 0"""
        , stderr='')
]