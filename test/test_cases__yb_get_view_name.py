test_cases = [
    test_case(
        cmd='yb_get_view_name.py @{argsdir}/db1 --schema dev --view a1_v'
        , exit_code=0
        , stdout='a1_v'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_view_name.py @{argsdir}/db1 --current_schema dev --schema '
            """'Prod' --view b1_v""")
        , exit_code=0
        , stdout='b1_v'
        , stderr=''),

    test_case(
        cmd=(
            "yb_get_view_name.py @{argsdir}/db1 --current_schema dev "
            """--schema Prod --view C1_v --database {db2} """)
        , exit_code=0
        , stdout='"C1_v"'
        , stderr=''),

    test_case(
        cmd=(
            "yb_get_view_name.py @{argsdir}/db1 --current_schema dev "
            """--schema Prod --view c1_v --database {db2} extra_pos_arg""")
        , exit_code=1
        , stdout="""usage: yb_get_view_name.py [options]

List/Verifies that the specified view exists.

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
  --view VIEW_NAME      view name

optional database object filter arguments:
  --owner OWNER_NAME    owner name
  --database DATABASE_NAME
                        database name
  --schema SCHEMA_NAME  schema name, defaults to CURRENT_SCHEMA

example usage:
  ./yb_get_view_name.py @$HOME/conn.args --schema Prod --view sales_v --

  file '$HOME/conn.args' contains:
    --host yb89
    --dbuser dze
    --conn_db stores"""
        , stderr="""yb_get_view_name.py: error: unrecognized arguments: extra_pos_arg""")
]