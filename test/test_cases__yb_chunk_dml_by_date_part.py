map_out = {
    r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}-\d{2}' : 'YYYY-MM-DD HH:MM:SS.FFFFFF-TZ'
    , r'\d{2}:\d{2}:\d{2}.\d{1,6}' : 'HH:MM:SS.FFFFFF'
    , r'\d{4}-\d{2}-\d{2}' : 'YYYY-MM-DD'}

test_cases = [
    test_case(
        cmd=('yb_chunk_dml_by_date_part.py @{argsdir}/yb_chunk_dml_by_date_part__args1 '
            '--execute_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-08-22 21:11:36.14636-06: Starting Date Part Chunking, first calculating HOUR group counts
--2020-08-22 21:11:36.745945-06: Build Chunk DMLs
--2020-08-22 21:11:36.746275-06: Chunk: 1, Rows: 327156, Range 2020-01-01 00:00:00 <= col12 < 2020-01-01 01:00:00
--2020-08-22 21:11:37.120013-06: Chunk: 2, Rows: 100284, Range 2020-01-01 01:00:00 <= col12 < 2020-01-03 00:00:00
--2020-08-22 21:11:37.412505-06: Chunk: 3, Rows: 100576, Range 2020-01-03 00:00:00 <= col12 < 2020-01-06 15:00:00
--2020-08-22 21:11:37.717729-06: Chunk: 4, Rows: 100288, Range 2020-01-06 15:00:00 <= col12 < 2020-01-12 10:00:00
--2020-08-22 21:11:38.019614-06: Chunk: 5, Rows: 100152, Range 2020-01-12 10:00:00 <= col12 < 2020-01-21 16:00:00
--2020-08-22 21:11:38.345962-06: Chunk: 6, Rows: 100040, Range 2020-01-21 16:00:00 <= col12 < 2020-02-06 10:00:00
--2020-08-22 21:11:38.590085-06: Chunk: 7, Rows: 100040, Range 2020-02-06 10:00:00 <= col12 < 2020-03-12 10:00:00
--2020-08-22 21:11:38.937236-06: Chunk: 8, Rows: 71464, Range 2020-03-12 10:00:00 <= col12 < 2021-12-02 15:00:00
--2020-08-22 21:11:39.338657-06: Chunk: 9, Rows: 0, col12 IS NULL
--2020-08-22 21:11:39.594577-06: Completed Date Part Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:03.449368
--Overhead duration  : 00:00:00.616575
--Total Chunks       : 9
--Min chunk size     : 100000
--Largest chunk size : 327156
--Average chunk size : 111111
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd=('yb_chunk_dml_by_date_part.py @{argsdir}/yb_chunk_dml_by_date_part__args1 '
            '--print_chunk_dml --null_chunk_off --verbose_chunk_off')
        , exit_code=0
        , stdout="""-- Running DML chunking.
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 327156) >>>*/ TO_TIMESTAMP('2020-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100284) >>>*/ TO_TIMESTAMP('2020-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-03 00:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 100576) >>>*/ TO_TIMESTAMP('2020-01-03 00:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-06 15:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100288) >>>*/ TO_TIMESTAMP('2020-01-06 15:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-12 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100152) >>>*/ TO_TIMESTAMP('2020-01-12 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-21 16:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100040) >>>*/ TO_TIMESTAMP('2020-01-21 16:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-02-06 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 100040) >>>*/ TO_TIMESTAMP('2020-02-06 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-03-12 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 71464) >>>*/ TO_TIMESTAMP('2020-03-12 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2021-12-02 15:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
-- Completed DML chunking."""
        , stderr='')

    , test_case(
        cmd=('yb_chunk_dml_by_date_part.py @{argsdir}/yb_chunk_dml_by_date_part__args1 '
            '--print_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-08-22 21:10:26.137851-06: Starting Date Part Chunking, first calculating HOUR group counts
--2020-08-22 21:10:26.59311-06: Build Chunk DMLs
--2020-08-22 21:10:26.5935-06: Chunk: 1, Rows: 327156, Range 2020-01-01 00:00:00 <= col12 < 2020-01-01 01:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 327156) >>>*/ TO_TIMESTAMP('2020-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.594261-06: Chunk: 2, Rows: 100284, Range 2020-01-01 01:00:00 <= col12 < 2020-01-03 00:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100284) >>>*/ TO_TIMESTAMP('2020-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-03 00:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.594503-06: Chunk: 3, Rows: 100576, Range 2020-01-03 00:00:00 <= col12 < 2020-01-06 15:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 100576) >>>*/ TO_TIMESTAMP('2020-01-03 00:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-06 15:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.594825-06: Chunk: 4, Rows: 100288, Range 2020-01-06 15:00:00 <= col12 < 2020-01-12 10:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100288) >>>*/ TO_TIMESTAMP('2020-01-06 15:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-12 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.595243-06: Chunk: 5, Rows: 100152, Range 2020-01-12 10:00:00 <= col12 < 2020-01-21 16:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100152) >>>*/ TO_TIMESTAMP('2020-01-12 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-01-21 16:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.596105-06: Chunk: 6, Rows: 100040, Range 2020-01-21 16:00:00 <= col12 < 2020-02-06 10:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100040) >>>*/ TO_TIMESTAMP('2020-01-21 16:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-02-06 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.597597-06: Chunk: 7, Rows: 100040, Range 2020-02-06 10:00:00 <= col12 < 2020-03-12 10:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 100040) >>>*/ TO_TIMESTAMP('2020-02-06 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2020-03-12 10:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.608436-06: Chunk: 8, Rows: 71464, Range 2020-03-12 10:00:00 <= col12 < 2021-12-02 15:00:00
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 71464) >>>*/ TO_TIMESTAMP('2020-03-12 10:00:00','YYYY-MM-DD HH24:MI:SS') <= col12 AND col12 < TO_TIMESTAMP('2021-12-02 15:00:00','YYYY-MM-DD HH24:MI:SS') /*<<< chunk_clause */;
--2020-08-22 21:10:26.608949-06: Chunk: 9, Rows: 0, col12 IS NULL
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE col12 IS NULL;
--2020-08-22 21:10:26.609202-06: Completed Date Part Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:00.47247
--Overhead duration  : 00:00:00.472545
--Total Chunks       : 9
--Min chunk size     : 100000
--Largest chunk size : 327156
--Average chunk size : 111111
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)
]