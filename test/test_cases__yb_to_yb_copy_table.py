map_out={r'\d{4}-[^S]*' : '', r'\s*\d{1,2}:\d{2}:\d{2}\s*\(.*' : ''}
test_cases = [
    test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 2560" """
            """ --src_table dev.data_types_t --dst_table "Prod".data_types_t --log_dir tmp"""
        , exit_code=0
        , stdout="""2020-10-11 16:32:54.529 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 2560 good rows in   0:00:06 (READ: 109.6KB/s WRITE: 58.92KB/s)"""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 2560" """
            """ --src_table dev.data_types_t --dst_table "Prod".data_types_t --chunk_rows 1000"""
            """ --delimiter '0x01' --log_dir tmp --log_prefix data_type_t_"""
        , exit_code=0
        , stdout="""2020-10-11 16:50:57.095 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 1000 good rows in   0:00:06 (READ: 43.59KB/s WRITE: 23.43KB/s)
2020-10-11 16:51:04.219 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 1000 good rows in   0:00:06 (READ: 43.88KB/s WRITE: 23.61KB/s)
2020-10-11 16:51:11.391 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 560 good rows in   0:00:06 (READ: 24.65KB/s WRITE: 13.22KB/s)"""
        , stderr=''
        , map_out=map_out)
]