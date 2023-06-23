test_cases = [
    test_case(
        cmd="""yb_convert_table_to_dist_replicate.py @{argsdir}/db1 --table_like '%b1%'"""
        , exit_code=0
        , stdout="""----------------------
-- Table: {db1}.dev.b1_t, Storage: 0MB, Distribute Replicate Convertion
----------------------
BEGIN;
ALTER TABLE {db1}.dev.b1_t RENAME TO b1_t__old;
CREATE TABLE {db1}.dev.b1_t (
    col1 INTEGER
)
DISTRIBUTE REPLICATE;
INSERT INTO {db1}.dev.b1_t SELECT * FROM {db1}.dev.b1_t__old;
DROP TABLE {db1}.dev.b1_t__old;
COMMIT;

----------------------
-- Table: {db1}."Prod".b1_t, Storage: 0MB, Distribute Replicate Convertion
----------------------
BEGIN;
ALTER TABLE {db1}."Prod".b1_t RENAME TO b1_t__old;
CREATE TABLE {db1}."Prod".b1_t (
    col1 INTEGER
)
DISTRIBUTE REPLICATE;
INSERT INTO {db1}."Prod".b1_t SELECT * FROM {db1}."Prod".b1_t__old;
DROP TABLE {db1}."Prod".b1_t__old;
COMMIT;"""
        , stderr="")
]