test_cases = [
    test_case(
        cmd='yb_get_table_distribution_key.py @{argsdir}/db1 --verbose 3'
        , exit_code=1
        , stdout="""usage: yb_get_table_distribution_key.py [options]

Identify the distribution column or type (random or replicated) of the requested table.

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
  ./yb_get_table_distribution_key.py @$HOME/conn.args --schema Prod --table sales --

  file '$HOME/conn.args' contains:
    --host yb89
    --dbuser dze
    --conn_db stores"""
        , stderr=(
        """yb_get_table_distribution_key.py: error: the following arguments are required: --table"""
        if self.test_py_version == 3
        else """yb_get_table_distribution_key.py: error: argument --table is required"""))

    , test_case(
        cmd=
        'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev --table a1_t'
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=
        'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev --table x1_t'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev '
            '--table dist_random_t')
        , exit_code=0
        , stdout='RANDOM'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev '
            '--table dist_replicate_t')
        , exit_code=0
        , stdout='REPLICATED'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --schema dev '
            '--table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr=''
        , comment=(
            'Use of sys.tables cross database does not work, so no value is '
            'returned. The table can be accessed but is returning no rows\nThe '
            'sys.vt_table_info has the same issue and also is only accessible '
            'by super users.'))

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --schema dev '
            '--table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --owner {user_name} '
            '--schema dev --table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --owner no_such_owner '
            '--schema dev --table a1_t --database {db2}')
        , exit_code=0
        , stdout=''
        , stderr='')
]