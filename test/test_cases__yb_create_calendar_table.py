test_cases = [
    test_case(
        cmd=("""yb_create_calendar_table.py @{argsdir}/db1 --table dev.myTestCalendar;"""
            " %s") %
                ("""$env:YBPASSWORD='{user_password}'; ybsql -h {host} -U {user_name} -d {db1} -c 'DROP TABLE dev.\""myTestCalendar\\""' 2> $null"""
                if Common.is_windows
                else """YBPASSWORD={user_password} ybsql -h {host} -U {user_name} -d {db1} -c 'DROP TABLE dev."myTestCalendar"' 2> /dev/null""")
        , exit_code=0
        , stdout="""--Creating calendar table: dev."myTestCalendar"
--Table created
DROP TABLE"""
        , stderr='')
    , test_case(
        cmd="""yb_create_calendar_table.py @{argsdir}/db1 --table sys.table;"""
        , exit_code=(0 if Common.is_windows else 1)
        , stdout=''
        , stderr=''
        , map_out = [ { 'regex' : re.compile(r'.*'), 'sub' : ''} ] )
]
