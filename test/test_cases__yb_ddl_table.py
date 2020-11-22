test_cases = [
    test_case(
        cmd='yb_ddl_table.py @{argsdir}/db1 --current_schema dev --table_like a1_t'
        , exit_code=0
        , stdout="""CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev Prod --table_like a1_t""")
        , exit_code=0
        , stdout="""CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev Prod --with_schema --table_like a1_t""")
        , exit_code=0
        , stdout="""CREATE TABLE dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE "Prod".a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --current_schema dev '
            """--schema_in dev Prod --with_db --table_like a1_t""")
        , exit_code=0
        , stdout="""CREATE TABLE {db1}.dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);

CREATE TABLE {db1}."Prod".a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr='')

    , test_case(
        cmd=
            ('yb_ddl_table.py @{argsdir}/db1 --schema_in dev '
            """--with_db  --with_rowcount --table_in data_types_t""")
        , exit_code=0
        , stdout="""--Rowcount: 1,000,000  Table: {db1}.dev.data_types_t  At: 2020-09-25 16:47:47.103207-07
CREATE TABLE {db1}.dev.data_types_t (
    col1 BIGINT,
    col2 INTEGER,
    col3 SMALLINT,
    col4 NUMERIC(18,0),
    col5 REAL,
    col6 DOUBLE PRECISION,
    col7 UUID,
    col8 CHARACTER VARYING(256),
    col9 CHARACTER(1),
    col10 DATE,
    col11 TIME WITHOUT TIME ZONE,
    col12 TIMESTAMP WITHOUT TIME ZONE,
    col13 TIMESTAMP WITH TIME ZONE,
    col14 IPV4,
    col15 IPV6,
    col16 MACADDR,
    col17 MACADDR8,
    col18 BOOLEAN,
    col19 INTEGER
)
DISTRIBUTE ON (col1);"""
        , stderr=''
        , map_out={r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^\s]*' : 'YYYY-MM-DD HH:MM:SS'})
]