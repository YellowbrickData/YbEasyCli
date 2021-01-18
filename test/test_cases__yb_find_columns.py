test_cases = [
    test_case(
        cmd=
            """yb_find_columns.py @{argsdir}/db1 --schema_in dev Prod --datatype_like 'CHAR%%' 'TIME%%'"""
        , exit_code=0
        , stdout="""-- Running: yb_find_columns
-- Table: {db1}.dev.data_types_t, Column: col8, Table Ordinal: 8, Data Type: CHARACTER VARYING(256)
-- Table: {db1}.dev.data_types_t, Column: col9, Table Ordinal: 9, Data Type: CHARACTER(1)
-- Table: {db1}.dev.data_types_t, Column: col11, Table Ordinal: 11, Data Type: TIME WITHOUT TIME ZONE
-- Table: {db1}.dev.data_types_t, Column: col12, Table Ordinal: 12, Data Type: TIMESTAMP WITHOUT TIME ZONE
-- Table: {db1}.dev.data_types_t, Column: col13, Table Ordinal: 13, Data Type: TIMESTAMP WITH TIME ZONE
-- Table: {db1}."Prod".data_types_t, Column: col8, Table Ordinal: 8, Data Type: CHARACTER VARYING(256)
-- Table: {db1}."Prod".data_types_t, Column: col9, Table Ordinal: 9, Data Type: CHARACTER(1)
-- Table: {db1}."Prod".data_types_t, Column: col11, Table Ordinal: 11, Data Type: TIME WITHOUT TIME ZONE
-- Table: {db1}."Prod".data_types_t, Column: col12, Table Ordinal: 12, Data Type: TIMESTAMP WITHOUT TIME ZONE
-- Table: {db1}."Prod".data_types_t, Column: col13, Table Ordinal: 13, Data Type: TIMESTAMP WITH TIME ZONE
-- 10 column/s found"""
        , stderr='')

    , test_case(
        cmd=
            """yb_find_columns.py @{argsdir}/db1 --schema_in dev --datatype_like 'CHAR%%' 'TIME%%'"""
        , exit_code=0
        , stdout="""-- Running: yb_find_columns
-- Table: {db1}.dev.data_types_t, Column: col8, Table Ordinal: 8, Data Type: CHARACTER VARYING(256)
-- Table: {db1}.dev.data_types_t, Column: col9, Table Ordinal: 9, Data Type: CHARACTER(1)
-- Table: {db1}.dev.data_types_t, Column: col11, Table Ordinal: 11, Data Type: TIME WITHOUT TIME ZONE
-- Table: {db1}.dev.data_types_t, Column: col12, Table Ordinal: 12, Data Type: TIMESTAMP WITHOUT TIME ZONE
-- Table: {db1}.dev.data_types_t, Column: col13, Table Ordinal: 13, Data Type: TIMESTAMP WITH TIME ZONE
-- 5 column/s found"""
        , stderr='')

    , test_case(
        cmd=
            """yb_find_columns.py @{argsdir}/db1 --schema_in dev --datatype_like 'CHAR%%' 'TIME%%'"""
            """ --output_template '{{ordinal}} of {{max_ordinal}}: {{column_path}}'"""
        , exit_code=0
        , stdout="""-- Running: yb_find_columns
1 of 5: {db1}.dev.data_types_t.col8
2 of 5: {db1}.dev.data_types_t.col9
3 of 5: {db1}.dev.data_types_t.col11
4 of 5: {db1}.dev.data_types_t.col12
5 of 5: {db1}.dev.data_types_t.col13
-- 5 column/s found"""
        , stderr='')
]