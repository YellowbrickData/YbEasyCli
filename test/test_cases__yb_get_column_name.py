test_cases = [
    test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_t --column col1'
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_v --column col1'
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_name.py @{argsdir}/db1 --schema dev --object a1_t --column colXX'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_name.py @{argsdir}/db1 --schema dev '
            '--object data_types_t --column col10')
        , exit_code=0
        , stdout='col10'
        , stderr='')

    , test_case(
        cmd=
            """yb_get_column_name.py @{argsdir}/db1 --schema 'Prod' --object C1_t --column Col1 --database {db2}"""
        , exit_code=0
        , stdout='"Col1"'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_column_name.py @{argsdir}/db1 --schema dev {db2} --object a1_v '
            '--column col1 extra_arg')
        , exit_code=1
        , stdout="""usage: yb_get_column_name.py [options]

List/Verifies that the specified table/view column name if it exists.

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
  --object OBJECT_NAME  object name
  --column COLUMN_NAME  column name

optional database object filter arguments:
  --owner OWNER_NAME    owner name
  --database DATABASE_NAME
                        database name
  --schema SCHEMA_NAME  schema name, defaults to CURRENT_SCHEMA

example usage:
  ./yb_get_column_name.py @$HOME/conn.args --schema dev --object sales --column price --

  file '$HOME/conn.args' contains:
    --host yb89
    --dbuser dze
    --conn_db stores"""
        , stderr="""yb_get_column_name.py: error: unrecognized arguments: {db2} extra_arg""")
]