test_cases = [
    test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_t --column col1'
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_v --column col1'
        , exit_code=0
        , stdout='col1'
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
        , stdout='col10'
        , stderr='')

    , test_case(
        cmd=
            """yb_get_column_name.py @{argsdir}/db1 --schema 'Prod' --object C1_t --column Col1 -- {db2}"""
        , exit_code=0
        , stdout='"Col1"'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_name.py @{argsdir}/db1 --schema dev {db2} --object a1_v '
            '--column col1 extra_arg')
        , exit_code=2
        , stdout=''
        , stderr="""usage: yb_get_column_name.py [database] [options]
yb_get_column_name.py: error: unrecognized arguments: extra_arg""")
]