test_cases = [
    test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col1'
        , exit_code=0
        , stdout='BIGINT'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col2'
        , exit_code=0
        , stdout='INTEGER'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col3'
        , exit_code=0
        , stdout='SMALLINT'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col4'
        , exit_code=0
        , stdout='NUMERIC(18,0)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col5'
        , exit_code=0
        , stdout='REAL'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col6'
        , exit_code=0
        , stdout='DOUBLE PRECISION'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col7'
        , exit_code=0
        , stdout='UUID'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col8'
        , exit_code=0
        , stdout='CHARACTER VARYING(256)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col9'
        , exit_code=0
        , stdout='CHARACTER(1)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col10'
        , exit_code=0
        , stdout='DATE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col11'
        , exit_code=0
        , stdout='TIME WITHOUT TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col12'
        , exit_code=0
        , stdout='TIMESTAMP WITHOUT TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col13'
        , exit_code=0
        , stdout='TIMESTAMP WITH TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col14'
        , exit_code=0
        , stdout='IPV4'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col15'
        , exit_code=0
        , stdout='IPV6'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col16'
        , exit_code=0
        , stdout='MACADDR'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col17'
        , exit_code=0
        , stdout='MACADDR8'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column colXX'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=('yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col1 --database {db2}')
        , exit_code=0
        , stdout='BIGINT'
        , stderr='')

    , test_case(
        cmd='yb_get_column_type.py @{argsdir}/db1 --schema dev col1'
        , exit_code=1
        , stdout="""usage: yb_get_column_type.py [options]

Return the data type of the requested column.

optional argument file/s:
  @arg_file             file containing arguments
                        to enter multi-line argument, use: --arg ""\"multi-line value""\"

optional arguments:
  --help, --usage, -u   display this help message and exit
  --verbose {{1,2,3}}     display verbose execution{{1 - info, 2 - debug, 3 - extended}}
  --nocolor             turn off colored text output
  --version, -v         display the program version and exit

connection arguments:
  --host HOST, -h HOST, -H HOST
                        database server hostname, overrides YBHOST env variable
  --port PORT, -p PORT, -P PORT
                        database server port, overrides YBPORT env variable, the default port is
                        5432
  --dbuser DBUSER, -U DBUSER
                        database user, overrides YBUSER env variable
  --conn_db CONN_DB, --db CONN_DB, -d CONN_DB, -D CONN_DB
                        database to connect to, overrides YBDATABASE env variable
  --current_schema CURRENT_SCHEMA
                        current schema after db connection
  -W                    prompt for password instead of using the YBPASSWORD env variable

required database object filter arguments:
  --table TABLE_NAME    table name
  --column COLUMN_NAME  column name

optional database object filter arguments:
  --owner OWNER_NAME    owner name
  --database DATABASE_NAME
                        database name
  --schema SCHEMA_NAME  schema name, defaults to CURRENT_SCHEMA

example usage:
  ./yb_get_column_type.py @$HOME/conn.args --schema dev --table sales --column price --

  file '$HOME/conn.args' contains:
    --host yb14
    --dbuser dze
    --conn_db stores"""
        , stderr=(
        """yb_get_column_type.py: error: the following arguments are required: --table, --column"""
        if self.test_py_version == 3
        else """yb_get_column_type.py: error: argument --table is required"""))
]