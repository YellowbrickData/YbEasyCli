map_out = {
    r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}-\d{2}' : 'YYYY-MM-DD HH:MM:SS.FFFFFF-TZ'
    , r'\d{2}:\d{2}:\d{2}.\d{1,6}' : 'HH:MM:SS.FFFFFF'
    , r'\d{4}-\d{2}-\d{2}' : 'YYYY-MM-DD'}

test_cases = [
    test_case(
        cmd=('yb_chunk_dml_by_yyyymmdd_integer.py @{argsdir}/yb_chunk_dml_by_yyyymmdd_integer__args1 '
            '--execute_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Starting YYYYMMDD Integer Date Chunking, first calculating date group counts
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Build Chunk DMLs
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 1, Rows: 166582, Range 20200101 <= col19 < 20200111
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 2, Rows: 100018, Range 20200111 <= col19 < 20200902
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 3, Rows: 101800, Range 20200902 <= col19 < 20210426
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 4, Rows: 100376, Range 20210426 <= col19 < 20211215
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 5, Rows: 100212, Range 20211215 <= col19 < 20220727
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 6, Rows: 100988, Range 20220727 <= col19 < 20230415
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 7, Rows: 102860, Range 20230415 <= col19 < 20240222
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 8, Rows: 100266, Range 20240222 <= col19 < 20250401
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 9, Rows: 100036, Range 20250401 <= col19 < 20320311
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 10, Rows: 26862, Range 20320311 <= col19 < 20420307
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Chunk: 11, Rows: 0, col19 IS NULL
--YYYY-MM-DD HH:MM:SS.FFFFFF-TZ: Completed YYYYMMDD Integer Date Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : HH:MM:SS.FFFFFF
--Overhead duration  : HH:MM:SS.FFFFFF
--Total Chunks       : 11
--Min chunk size     : 100000
--Largest chunk size : 166582
--Average chunk size : 90909
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd=('yb_chunk_dml_by_yyyymmdd_integer.py @{argsdir}/yb_chunk_dml_by_yyyymmdd_integer__args1 '
            '--print_chunk_dml --null_chunk_off --verbose_chunk_off')
        , exit_code=0
        , stdout="""-- Running DML chunking.
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 166582) >>>*/ 20200101 <= col19 AND col19 < 20200111 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100018) >>>*/ 20200111 <= col19 AND col19 < 20200902 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 101800) >>>*/ 20200902 <= col19 AND col19 < 20210426 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100376) >>>*/ 20210426 <= col19 AND col19 < 20211215 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100212) >>>*/ 20211215 <= col19 AND col19 < 20220727 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100988) >>>*/ 20220727 <= col19 AND col19 < 20230415 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 102860) >>>*/ 20230415 <= col19 AND col19 < 20240222 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 100266) >>>*/ 20240222 <= col19 AND col19 < 20250401 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 9, size: 100036) >>>*/ 20250401 <= col19 AND col19 < 20320311 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 10, size: 26862) >>>*/ 20320311 <= col19 AND col19 < 20420307 /*<<< chunk_clause */;
-- Completed DML chunking."""
        , stderr='')

    , test_case(
        cmd=('yb_chunk_dml_by_yyyymmdd_integer.py @{argsdir}/yb_chunk_dml_by_yyyymmdd_integer__args1 '
            '--print_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-08-22 23:04:57.77992-06: Starting YYYYMMDD Integer Date Chunking, first calculating date group counts
--2020-08-22 23:04:58.202254-06: Build Chunk DMLs
--2020-08-22 23:04:58.202609-06: Chunk: 1, Rows: 166582, Range 20200101 <= col19 < 20200111
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 166582) >>>*/ 20200101 <= col19 AND col19 < 20200111 /*<<< chunk_clause */;
--2020-08-22 23:04:58.203502-06: Chunk: 2, Rows: 100018, Range 20200111 <= col19 < 20200902
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100018) >>>*/ 20200111 <= col19 AND col19 < 20200902 /*<<< chunk_clause */;
--2020-08-22 23:04:58.203782-06: Chunk: 3, Rows: 101800, Range 20200902 <= col19 < 20210426
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 101800) >>>*/ 20200902 <= col19 AND col19 < 20210426 /*<<< chunk_clause */;
--2020-08-22 23:04:58.204023-06: Chunk: 4, Rows: 100376, Range 20210426 <= col19 < 20211215
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100376) >>>*/ 20210426 <= col19 AND col19 < 20211215 /*<<< chunk_clause */;
--2020-08-22 23:04:58.204269-06: Chunk: 5, Rows: 100212, Range 20211215 <= col19 < 20220727
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100212) >>>*/ 20211215 <= col19 AND col19 < 20220727 /*<<< chunk_clause */;
--2020-08-22 23:04:58.204521-06: Chunk: 6, Rows: 100988, Range 20220727 <= col19 < 20230415
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100988) >>>*/ 20220727 <= col19 AND col19 < 20230415 /*<<< chunk_clause */;
--2020-08-22 23:04:58.204862-06: Chunk: 7, Rows: 102860, Range 20230415 <= col19 < 20240222
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 102860) >>>*/ 20230415 <= col19 AND col19 < 20240222 /*<<< chunk_clause */;
--2020-08-22 23:04:58.205211-06: Chunk: 8, Rows: 100266, Range 20240222 <= col19 < 20250401
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 100266) >>>*/ 20240222 <= col19 AND col19 < 20250401 /*<<< chunk_clause */;
--2020-08-22 23:04:58.207026-06: Chunk: 9, Rows: 100036, Range 20250401 <= col19 < 20320311
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 9, size: 100036) >>>*/ 20250401 <= col19 AND col19 < 20320311 /*<<< chunk_clause */;
--2020-08-22 23:04:58.207984-06: Chunk: 10, Rows: 26862, Range 20320311 <= col19 < 20420307
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 10, size: 26862) >>>*/ 20320311 <= col19 AND col19 < 20420307 /*<<< chunk_clause */;
--2020-08-22 23:04:58.208485-06: Chunk: 11, Rows: 0, col19 IS NULL
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE col19 IS NULL;
--2020-08-22 23:04:58.208789-06: Completed YYYYMMDD Integer Date Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:00.430099
--Overhead duration  : 00:00:00.430176
--Total Chunks       : 11
--Min chunk size     : 100000
--Largest chunk size : 166582
--Average chunk size : 90909
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)
]
