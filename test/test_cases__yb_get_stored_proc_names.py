test_cases = [
    test_case(cmd='yb_get_stored_proc_names.py @{argsdir}/db1 --schema_in dev Prod'
        , exit_code=0
        , stdout="""{db1}.dev.get_data_types_p
{db1}.dev.query_definer_p
{db1}.dev.test_error_p
{db1}.dev."test_Raise_p"
{db1}."Prod".get_data_types_p
{db1}."Prod".query_definer_p
{db1}."Prod".test_error_p
{db1}."Prod"."test_Raise_p" """
        , stderr='')

    , test_case(cmd='yb_get_stored_proc_names.py @{argsdir}/db1 --schema_in Prod --stored_proc_in test_Raise_p'
        , exit_code=0
        , stdout="""{db1}."Prod"."test_Raise_p" """
        , stderr='')

    , test_case(cmd="yb_get_stored_proc_names.py @{argsdir}/db1 --schema_in dev Prod --output_template 'database: {{database}}, schema: {{schema}}, proc: {{stored_proc}}'"
        , exit_code=0
        , stdout="""database: {db1}, schema: dev, proc: get_data_types_p
database: {db1}, schema: dev, proc: query_definer_p
database: {db1}, schema: dev, proc: test_error_p
database: {db1}, schema: dev, proc: "test_Raise_p"
database: {db1}, schema: "Prod", proc: get_data_types_p
database: {db1}, schema: "Prod", proc: query_definer_p
database: {db1}, schema: "Prod", proc: test_error_p
database: {db1}, schema: "Prod", proc: "test_Raise_p" """
        , stderr='')

    , test_case(cmd='yb_get_stored_proc_names.py --help'
        , exit_code=0
        , stdout="%s%s%s" % ("""usage: yb_get_stored_proc_names.py [options]

List/Verifies that the specified stored procedure/s exist.

optional argument file/s:
  @arg_file             file containing arguments
                        to enter multi-line argument, use: --arg \"\"\"multi-line value\"\"\"

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

optional output arguments:
  --output_template template
                        template used to print output, defaults to '{{stored_proc_path}}', template
                        variables include; {{stored_proc_path}}, {{schema_path}}, {{stored_proc}},
                        {{schema}}, {{database}}, {{owner}}
  --exec_output         execute output as SQL, defaults to FALSE

optional database object filter arguments:
  --database DATABASE_NAME
                        database name
  --owner_in OWNER_NAME [OWNER_NAME ...]
                        owner/s in the list
  --owner_NOTin OWNER_NAME [OWNER_NAME ...]
                        owner/s NOT in the list
  --owner_like PATTERN [PATTERN ...]
                        owner/s like the pattern/s
  --owner_NOTlike PATTERN [PATTERN ...]
                        owner/s NOT like the pattern/s
  --schema_in SCHEMA_NAME [SCHEMA_NAME ...]
                        schema/s in the list
  --schema_NOTin SCHEMA_NAME [SCHEMA_NAME ...]
                        schema/s NOT in the list
  --schema_like PATTERN [PATTERN ...]
                        schema/s like the pattern/s
  --schema_NOTlike PATTERN [PATTERN ...]
                        schema/s NOT like the pattern/s
  --stored_proc_in STORED_PROC_NAME [STORED_PROC_NAME ...]
                        stored_proc/s in the list
  --stored_proc_NOTin STORED_PROC_NAME [STORED_PROC_NAME ...]
                        stored_proc/s NOT in the list
  --stored_proc_like PATTERN [PATTERN ...]
                        stored_proc/s like the pattern/s
  --stored_proc_NOTlike PATTERN [PATTERN ...]
                        stored_proc/s NOT like the pattern/s

example usage:
"""
, ("""  python yb_get_stored_proc_names.py '@$HOME/conn.args' --schema_in dev Prod --stored_proc_like '%price%' --stored_proc_NOTlike '%id%' --"""
if Common.is_windows
else """  yb_get_stored_proc_names.py @$HOME/conn.args --schema_in dev Prod --stored_proc_like '%price%' --stored_proc_NOTlike '%id%' --""")
, """

  file '$HOME/conn.args' contains:
    --host yb89
    --dbuser dze
    --conn_db stores""")
    , stderr=''
    , map_out=[ { 'regex' : re.compile(r'optional arguments\:'), 'sub' : 'options:'} ] )
]