test_cases = [
    test_case(cmd='yb_chunk_optimal_rows.py @{argsdir}/db1 --table data_typesx_t --schema dev'
        , exit_code=1
        , stdout="""None"""
        , stderr='')
 
    , test_case(cmd='yb_chunk_optimal_rows.py @{argsdir}/db1 --table data_types_t --schema dev'
        , exit_code=0
        , stdout="""10000000"""
        , stderr='')
 
    , test_case(cmd='yb_chunk_optimal_rows.py @{argsdir}/db1 --table data_types_t --current_schema dev'
        , exit_code=0
        , stdout="""10000000"""
        , stderr='')
]