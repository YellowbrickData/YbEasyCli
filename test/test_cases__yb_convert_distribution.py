test_cases = [
    test_case(
        cmd="""yb_convert_distribution.py @{argsdir}/db1 --distribute REPLICATE --table_like '%b1%'"""
        , exit_code=0
        , stdout="""----------------------
-- Table: {db1}.dev.b1_t, Storage: 0MB, Distribute REPLICATE Conversion
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
-- Table: {db1}."Prod".b1_t, Storage: 0MB, Distribute REPLICATE Conversion
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
    , test_case(
        cmd="""yb_convert_distribution.py @{argsdir}/db1 --distribute RANDOM --min 0 --max 1000 --table_like '%b1%'"""
        , exit_code=0
        , stdout="""----------------------
-- Table: {db1}.dev.b1_t, Storage: 0MB, Distribute RANDOM Conversion
----------------------
BEGIN;
ALTER TABLE {db1}.dev.b1_t RENAME TO b1_t__old;
CREATE TABLE {db1}.dev.b1_t (
    col1 INTEGER
)
DISTRIBUTE RANDOM;
INSERT INTO {db1}.dev.b1_t SELECT * FROM {db1}.dev.b1_t__old;
DROP TABLE {db1}.dev.b1_t__old;
COMMIT;

----------------------
-- Table: {db1}."Prod".b1_t, Storage: 0MB, Distribute RANDOM Conversion
----------------------
BEGIN;
ALTER TABLE {db1}."Prod".b1_t RENAME TO b1_t__old;
CREATE TABLE {db1}."Prod".b1_t (
    col1 INTEGER
)
DISTRIBUTE RANDOM;
INSERT INTO {db1}."Prod".b1_t SELECT * FROM {db1}."Prod".b1_t__old;
DROP TABLE {db1}."Prod".b1_t__old;
COMMIT;"""
        , stderr="")
]