map_out={r'\d{4}-[^S]*' : ''
    , r'\s*\d{1,2}:\d{2}:\d{2}\s*\(.*' : ''
    , r'\d\.\d\.\d-\d{1,5}' : 'X.X.X-XXXXX'}
test_cases = [
    test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 2560" """
            """ --src_table dev.data_types_t --dst_table Prod.data_types_t --log_dir tmp"""
        , exit_code=0
        , stdout="""-- chunk1of1
2020-10-11 16:32:54.529 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 2560 good rows in   0:00:06 (READ: 109.6KB/s WRITE: 58.92KB/s)"""
        , stderr=''
        , map_out=map_out)

   , test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 100" """
            """ --src_table dev.data_types_t --dst_table Prod.data_types_t --log_dir tmp --create_dst_table"""
        , exit_code=(0 if Common.is_windows else 3)
        , stdout=''
        , stderr='yb_to_yb_copy_table.py: ERROR:  relation "data_types_t" already exists'
        , map_out=map_out)

   , test_case(
        cmd=("""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 100" """
            """ --src_table dev.data_types_t --dst_table Prod.data_types_100_t --log_dir tmp --create_dst_table;"""
            """ %s""") %
                ("""$env:YBPASSWORD='{user_password}'; ybsql -h {host} -U {user_name} -d {db2} -c 'DROP TABLE \""Prod\\"".data_types_100_t'"""
                if Common.is_windows
                else """YBPASSWORD={user_password} ybsql -h {host} -U {user_name} -d {db2} -c 'DROP TABLE "Prod".data_types_100_t'""")
        , exit_code=0
        , stdout="""-- created destination table: Prod.data_types_100_t
-- chunk1of1
2021-03-01 21:04:52.988 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 100 good rows in   0:00:06 (READ:  4.15KB/s WRITE:  2.30KB/s)
DROP TABLE"""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 2560" """
            """ --src_table dev.data_types_t --dst_table Prod.data_types_t --chunk_rows 1000"""
            """ --delimiter '0x01' --log_dir tmp --log_prefix data_type_t_"""
        , exit_code=1
        , stdout=""
        , stderr="""yb_to_yb_copy_table.py: The '--chunk_rows' option is only supported on YBDB version 4 or higher. The source db is running YBDB 3.3.2-23096..."""
        , map_out=map_out)
        if self.ybdb_version_major < 4
        else test_case(
            cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 2560" """
                """ --src_table dev.data_types_t --dst_table Prod.data_types_t --chunk_rows 1000"""
                """ --delimiter '0x01' --log_dir tmp --log_prefix data_type_t_"""
            , exit_code=0
            , stdout="""-- chunk1of3
2020-10-11 16:50:57.095 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 1000 good rows in   0:00:06 (READ: 43.59KB/s WRITE: 23.43KB/s)
-- chunk2of3
2020-10-11 16:51:04.219 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 1000 good rows in   0:00:06 (READ: 43.88KB/s WRITE: 23.61KB/s)
-- chunk3of3
2020-10-11 16:51:11.391 [ INFO] <main>  SUCCESSFUL BULK LOAD: Loaded 560 good rows in   0:00:06 (READ: 24.65KB/s WRITE: 13.22KB/s)"""
            , stderr=''
            , map_out=map_out)

   , test_case(
        cmd="""yb_to_yb_copy_table.py @{argsdir}/src_db1_dst_db2 --unload_where_clause "col1 <= 100" """
            """ --src_table dev.data_types_t --dst_table Prod.data_types_t --log_dir tmp --threads 3"""
        , exit_code=(0 if Common.is_windows else 1)
        , stdout=''
        , stderr="yb_to_yb_copy_table.py: The '--threads' option is only supported for YBDB super users."
        , map_out=map_out)
]
