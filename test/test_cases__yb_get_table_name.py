test_cases = [
    test_case(
        cmd='yb_get_table_name.py @{argsdir}/db1 --current_schema dev --table a1_t'
        , exit_code=0
        , stdout="""a1_t"""
        , stderr='')

    , test_case(
        cmd='yb_get_table_name.py @{argsdir}/db1 --schema dev --table a1_t'
        , exit_code=0
        , stdout='a1_t'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_name.py @{argsdir}/db1 --current_schema dev --schema '
            "'Prod' --table b1_t")
        , exit_code=0
        , stdout='b1_t'
        , stderr='')

    , test_case(
        cmd=(
            """yb_get_table_name.py @{argsdir}/db1 --current_schema dev --schema """
            """'Prod' --table C1_t --database {db2}""")
        , exit_code=0
        , stdout='"C1_t"'
        , stderr='')

    , test_case(
        cmd=(
            """yb_get_table_name.py @{argsdir}/db1 --current_schema dev --schema """
            """'Prod' --table C1_t --database {db2} extra_pos_arg""")
        , exit_code=1
        , stdout="""usage: yb_get_table_name.py [options]

List/Verifies that the specified table exists.

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

optional database object filter arguments:
  --owner OWNER_NAME    owner name
  --database DATABASE_NAME
                        database name
  --schema SCHEMA_NAME  schema name, defaults to CURRENT_SCHEMA

example usage:
  ./yb_get_table_name.py @$HOME/conn.args --current_schema dev --table sales --

  file '$HOME/conn.args' contains:
    --host yb89
    --dbuser dze
    --conn_db stores"""
        , stderr="""yb_get_table_name.py: error: unrecognized arguments: extra_pos_arg""")
]