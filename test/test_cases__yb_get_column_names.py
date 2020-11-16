test_cases = [
    test_case(
        cmd='yb_get_column_names.py @{argsdir}/db1 --schema dev -- a1_t'
        , exit_code=0
        , stdout="""col1"""
        , stderr='')

    , test_case(
        cmd='yb_get_column_names.py @{argsdir}/db1 --schema dev -- a1_v'
        , exit_code=0
        , stdout="""col1"""
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_names.py @{argsdir}/db1 --schema dev'
            ' -- data_types_t')
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
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema dev"
            " --column_NOTlike '%%1%%' -- 'data_types_t'")
        , exit_code=0
        , stdout="""col2
col3
col4
col5
col6
col7
col8
col9"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema Prod"
            " -- {db2} C1_t")
        , exit_code=0
        , stdout='"Col1"'
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema dev"
            """ --output_template "SELECT 'MAX <column> value: ' || MAX(<column> || '') FROM <table_path>;" """
            " --exec_output -- data_types_t")
        , exit_code=0
        , stdout="""MAX col1 value: 999999
MAX col2 value: 999999
MAX col3 value: 9999
MAX col4 value: 999999000
MAX col5 value: 9e+07
MAX col6 value: 999998.990001
MAX col7 value: 12345678-90ab-cdef-1234-567891000000
MAX col8 value: {{{{u)
MAX col9 value: |
MAX col10 value: 2042-03-06
MAX col11 value: 03:16:01
MAX col12 value: 2021-12-02 14:48:01
MAX col13 value: 2021-12-02 17:48:01-08
MAX col14 value: 90.90.84.8
MAX col15 value: 0090:0090:0084:0008:0090:0090:0084:0008
MAX col16 value: 90:90:84:08:90:90
MAX col17 value: 90:90:84:08:90:90:84:08
MAX col18 value: true
MAX col19 value: 20420306"""
        , stderr='')
]