test_cases = [
    test_case(
        cmd="yb_get_column_names.py @{argsdir}/db1 --schema_in dev --object_in a1_t --database_like '%{user_name}%'"
        , exit_code=0
        , stdout="""{db1}.dev.a1_t.col1
{db2}.dev.a1_t.col1"""
        , stderr='')

    , test_case(
        cmd="yb_get_column_names.py @{argsdir}/db1 --schema_in dev --object_in a1_v --database_like '%{user_name}%'"
        , exit_code=0
        , stdout="""{db1}.dev.a1_v.col1
{db2}.dev.a1_v.col1"""
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema_in dev"
            " --object_in data_types_t --database_in {db1} --output '{{column}}'")
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
            "yb_get_column_names.py @{argsdir}/db1 --schema_in dev"
            " --column_NOTlike '%%1%%' --database_in {db1} --object_in data_types_t --output '{{column}}'")
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
            "yb_get_column_names.py @{argsdir}/db1 --schema_in Prod"
            " --database_in {db2} --object_in C1_t --output '{{column}}'")
        , exit_code=0
        , stdout='"Col1"'
        , stderr='')

    , test_case(
        cmd=(
            "yb_get_column_names.py @{argsdir}/db1 --schema_in dev --database_in {db1}"
            """ --output_template "SELECT 'MAX {{column}} value: ' || MAX({{column}} || '') FROM {{object_path}};" """
            " --exec_output --object_in data_types_t")
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
        , stderr=''
        , map_out=[ { 'regex' : re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^\s]*'), 'sub' : 'YYYY-MM-DD HH:MM:SS' } ] )
]