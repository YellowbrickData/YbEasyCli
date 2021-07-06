map_out = {
    r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}(-|\+)\d{2}' : 'YYYY-MM-DD HH:MM:SS.FFFFFF-TZ'
    , r'\d{2}:\d{2}:\d{2}.\d{1,6}' : 'HH:MM:SS.FFFFFF'
    , r'\d{4}-\d{2}-\d{2}' : 'YYYY-MM-DD'}

test_cases = [
    test_case(
        cmd=('yb_chunk_dml_by_integer.py @{argsdir}/yb_chunk_dml_by_integer__args1 '
            '--column col4 --execute_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-08-22 18:19:38.201736-06: Starting Integer Chunking, first calculating group counts
--2020-08-22 18:19:38.301736-06: Build Chunk Groupings, first pass
--2020-08-22 18:19:39.431422-06: Build Chunk DMLs
--2020-08-22 18:19:39.522147-06: Chunk: 1, Rows: 100000, Range 1000000 <= col4 < 47500950000
--2020-08-22 18:19:39.822828-06: Chunk: 2, Rows: 100000, Range 47500950000 <= col4 < 90000900000
--2020-08-22 18:19:40.154894-06: Chunk: 3, Rows: 100000, Range 90000900000 <= col4 < 127500850000
--2020-08-22 18:19:40.462646-06: Chunk: 4, Rows: 100000, Range 127500850000 <= col4 < 160000800000
--2020-08-22 18:19:40.781904-06: Chunk: 5, Rows: 100000, Range 160000800000 <= col4 < 187500750000
--2020-08-22 18:19:41.121436-06: Chunk: 6, Rows: 100000, Range 187500750000 <= col4 < 210000700000
--2020-08-22 18:19:41.398286-06: Chunk: 7, Rows: 100000, Range 210000700000 <= col4 < 227500650000
--2020-08-22 18:19:41.758007-06: Chunk: 8, Rows: 100000, Range 227500650000 <= col4 < 240000600000
--2020-08-22 18:19:42.12159-06: Chunk: 9, Rows: 100000, Range 240000600000 <= col4 < 247500550000
--2020-08-22 18:19:42.432212-06: Chunk: 10, Rows: 100000, Range 247500550000 <= col4 < 250000500001
--2020-08-22 18:19:42.672871-06: Chunk: 11, Rows: 0, col4 IS NULL
--2020-08-22 18:19:42.916537-06: Completed Integer Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:04.71574
--Overhead duration  : 00:00:02.176085
--Total Chunks       : 11
--Min chunk size     : 100000
--Largest chunk size : 100000
--Average chunk size : 90909
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd=('yb_chunk_dml_by_integer.py @{argsdir}/yb_chunk_dml_by_integer__args1 '
            '--column col4 --print_chunk_dml --null_chunk_off --verbose_chunk_off')
        , exit_code=0
        , stdout="""-- Running DML chunking.
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 100000) >>>*/ 1000000 <= col4 AND col4 < 47500950000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100000) >>>*/ 47500950000 <= col4 AND col4 < 90000900000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 100000) >>>*/ 90000900000 <= col4 AND col4 < 127500850000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100000) >>>*/ 127500850000 <= col4 AND col4 < 160000800000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100000) >>>*/ 160000800000 <= col4 AND col4 < 187500750000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100000) >>>*/ 187500750000 <= col4 AND col4 < 210000700000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 100000) >>>*/ 210000700000 <= col4 AND col4 < 227500650000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 100000) >>>*/ 227500650000 <= col4 AND col4 < 240000600000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 9, size: 100000) >>>*/ 240000600000 <= col4 AND col4 < 247500550000 /*<<< chunk_clause */;
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 10, size: 100000) >>>*/ 247500550000 <= col4 AND col4 < 250000500001 /*<<< chunk_clause */;
-- Completed DML chunking."""
        , stderr='')

    , test_case(
        cmd=('yb_chunk_dml_by_integer.py @{argsdir}/yb_chunk_dml_by_integer__args1 '
            '--column col4 --print_chunk_dml')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-08-22 19:26:27.672082-06: Starting Integer Chunking, first calculating group counts
--2020-08-22 19:26:27.801736-06: Build Chunk Groupings, first pass
--2020-08-22 19:26:28.922245-06: Build Chunk DMLs
--2020-08-22 19:26:29.010727-06: Chunk: 1, Rows: 100000, Range 1000000 <= col4 < 47500950000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 1, size: 100000) >>>*/ 1000000 <= col4 AND col4 < 47500950000 /*<<< chunk_clause */;
--2020-08-22 19:26:29.321661-06: Chunk: 2, Rows: 100000, Range 47500950000 <= col4 < 90000900000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 2, size: 100000) >>>*/ 47500950000 <= col4 AND col4 < 90000900000 /*<<< chunk_clause */;
--2020-08-22 19:26:29.615457-06: Chunk: 3, Rows: 100000, Range 90000900000 <= col4 < 127500850000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 3, size: 100000) >>>*/ 90000900000 <= col4 AND col4 < 127500850000 /*<<< chunk_clause */;
--2020-08-22 19:26:29.913853-06: Chunk: 4, Rows: 100000, Range 127500850000 <= col4 < 160000800000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 4, size: 100000) >>>*/ 127500850000 <= col4 AND col4 < 160000800000 /*<<< chunk_clause */;
--2020-08-22 19:26:30.260152-06: Chunk: 5, Rows: 100000, Range 160000800000 <= col4 < 187500750000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 5, size: 100000) >>>*/ 160000800000 <= col4 AND col4 < 187500750000 /*<<< chunk_clause */;
--2020-08-22 19:26:30.536624-06: Chunk: 6, Rows: 100000, Range 187500750000 <= col4 < 210000700000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 6, size: 100000) >>>*/ 187500750000 <= col4 AND col4 < 210000700000 /*<<< chunk_clause */;
--2020-08-22 19:26:30.822253-06: Chunk: 7, Rows: 100000, Range 210000700000 <= col4 < 227500650000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 7, size: 100000) >>>*/ 210000700000 <= col4 AND col4 < 227500650000 /*<<< chunk_clause */;
--2020-08-22 19:26:31.15679-06: Chunk: 8, Rows: 100000, Range 227500650000 <= col4 < 240000600000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 8, size: 100000) >>>*/ 227500650000 <= col4 AND col4 < 240000600000 /*<<< chunk_clause */;
--2020-08-22 19:26:31.447927-06: Chunk: 9, Rows: 100000, Range 240000600000 <= col4 < 247500550000
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 9, size: 100000) >>>*/ 240000600000 <= col4 AND col4 < 247500550000 /*<<< chunk_clause */;
--2020-08-22 19:26:31.791157-06: Chunk: 10, Rows: 100000, Range 247500550000 <= col4 < 250000500001
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE /* chunk_clause(chunk: 10, size: 100000) >>>*/ 247500550000 <= col4 AND col4 < 250000500001 /*<<< chunk_clause */;
--2020-08-22 19:26:32.112984-06: Chunk: 11, Rows: 0, col4 IS NULL
INSERT INTO new_chunked_table SELECT * FROM {db1}.dev.data_types_t WHERE col4 IS NULL;
--2020-08-22 19:26:32.349486-06: Completed Integer Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:04.678549
--Overhead duration  : 00:00:02.19171
--Total Chunks       : 11
--Min chunk size     : 100000
--Largest chunk size : 100000
--Average chunk size : 90909
-- Completed DML chunking."""
        , stderr=''
        , map_out=map_out)

    , test_case(
        cmd=('yb_chunk_dml_by_integer.py @{argsdir}/yb_chunk_dml_by_integer__args1 '
            '--column col1 --column_cardinality high')
        , exit_code=0
        , stdout="""-- Running DML chunking.
--2020-12-25 21:16:02.899221-08: Starting Integer Chunking, first calculating group counts
--2020-12-25 21:16:03.053718-08: Build Chunk Groupings, first pass
--2020-12-25 21:16:03.278257-08: Build Chunk DMLs
--2020-12-25 21:16:03.280744-08: Chunk: 1, Rows: 100095, Range 1 <= col1 < 100096
--2020-12-25 21:16:03.283273-08: Chunk: 2, Rows: 100096, Range 100096 <= col1 < 200192
--2020-12-25 21:16:03.285346-08: Chunk: 3, Rows: 100096, Range 200192 <= col1 < 300288
--2020-12-25 21:16:03.287403-08: Chunk: 4, Rows: 100096, Range 300288 <= col1 < 400384
--2020-12-25 21:16:03.289469-08: Chunk: 5, Rows: 100096, Range 400384 <= col1 < 500480
--2020-12-25 21:16:03.291527-08: Chunk: 6, Rows: 100096, Range 500480 <= col1 < 600576
--2020-12-25 21:16:03.293583-08: Chunk: 7, Rows: 100096, Range 600576 <= col1 < 700672
--2020-12-25 21:16:03.295636-08: Chunk: 8, Rows: 100096, Range 700672 <= col1 < 800768
--2020-12-25 21:16:03.297689-08: Chunk: 9, Rows: 100096, Range 800768 <= col1 < 900864
--2020-12-25 21:16:03.299731-08: Chunk: 10, Rows: 99137, Range 900864 <= col1 < 1000001
--2020-12-25 21:16:03.300263-08: Chunk: 11, Rows: 0, col1 IS NULL
--2020-12-25 21:16:03.300573-08: Completed Integer Chunked DML
--Total Rows         : 1000000
--IS NULL Rows       : 0
--Running total check: PASSED
--Duration           : 00:00:00.402947
--Overhead duration  : 00:00:00.403053
--Total Chunks       : 11
--Min chunk size     : 100000
--Largest chunk size : 100096
--Average chunk size : 90909
-- Completed DML chunking.
"""
        , stderr=''
        , map_out=map_out)
]