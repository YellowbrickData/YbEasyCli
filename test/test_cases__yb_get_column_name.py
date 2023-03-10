test_cases = [
    test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_t --column col1'
        , exit_code=0
        , stdout='{db1}.dev.a1_t.col1'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_v --column col1'
        , exit_code=0
        , stdout='{db1}.dev.a1_v.col1'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_t --column colXX'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_name.py @{argsdir}/db1 --schema dev '
            '--object data_types_t --column col10')
        , exit_code=0
        , stdout='{db1}.dev.data_types_t.col10'
        , stderr='')

    , test_case(
        cmd=
            """yb_get_column_name.py @{argsdir}/db1 --schema 'Prod' --object C1_t --column Col1 --database {db2}"""
        , exit_code=0
        , stdout='{db2}."Prod"."C1_t"."Col1"'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_name.py @{argsdir}/db1 --schema dev {db2} --object a1_v '
            '--column col1 extra_arg')
        , exit_code=(0 if Common.is_windows else 1)
        , stdout=""
        , stderr="""yb_get_column_name.py: error: unrecognized arguments: {db2} extra_arg
for complete help, execute: yb_get_column_name.py --help""")
]