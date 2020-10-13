test_cases = [
    test_case(
        cmd="""yb_is_cstore_table.py @{argsdir}/db1 --table sys.blade"""
        , exit_code=0
        , stdout="""False"""
        , stderr='')

    , test_case(
        cmd="""yb_is_cstore_table.py @{argsdir}/db1 --table Prod.data_types_t"""
        , exit_code=0
        , stdout="""True"""
        , stderr='')

    , test_case(
        cmd="""yb_is_cstore_table.py @{argsdir}/db1 --table data_types_t"""
        , exit_code=2
        , stdout=""""""
        , stderr="""ERROR:  relation "data_types_t" does not exist""")
]