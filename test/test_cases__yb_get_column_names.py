test_cases = [
    test_case(
        cmd='yb_get_column_names.py @{argsdir}/db1 --schema dev -- a1_t'
        , exit_code=0
        , stdout="""col1"""
        , stderr=''),

    test_case(
        cmd='yb_get_column_names.py @{argsdir}/db1 --schema dev -- a1_v'
        , exit_code=0
        , stdout="""col1"""
        , stderr=''),

    test_case(
        cmd=(
            'yb_get_column_names.py @{argsdir}/db1 --schema dev '
            '-- data_types_t')
        , exit_code=0
        , stdout="""col1
col2
col3
col4
col5
col6
col7
col8
col9
col10
col11
col12
col13
col14
col15
col16
col17
col18
col19"""
        , stderr=''),

    test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema dev "
            "--column_NOTlike '%%1%%' -- 'data_types_t'")
        , exit_code=0
        , stdout="""col2
col3
col4
col5
col6
col7
col8
col9"""
        , stderr=''),

    test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema 'Prod' "
            "-- {db2} 'C1_t'")
        , exit_code=0
        , stdout='"Col1"'
        , stderr='')
]