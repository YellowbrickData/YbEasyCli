#!/usr/bin/env python3
"""Performs functions such as argument parsing, login verification, logging,
and command execution that are common to all utilities in this package.
"""

import argparse
import getpass
import os
import platform
import re
import subprocess
import sys
import traceback
import shlex
from datetime import datetime


class common:
    """This class contains functions used for argument parsing, login
    verification, logging, and command execution.
    """

    def __init__(self, connect_timeout=10):
        """Create an instance of the common library used by all utilities

        :param connect_timeout: database timeout in seconds when trying to
            connect, defaults to 10 seconds
        """
        self.database = None
        self.schema = None
        self.connect_timeout = connect_timeout

        self.version = '20200908'

        self.start_ts = datetime.now()

        self.util_dir_path = os.path.dirname(os.path.realpath(__file__))
        self.util_file_name = os.path.basename(os.path.realpath(__file__))
        self.util_name = self.util_file_name.split('.')[0]

    def formatter(self, prog):
        return argparse.RawDescriptionHelpFormatter(prog, width=100)

    def db_args(self
        , description
        , required_args_single=[]
        , optional_args_single=['schema']
        , optional_args_multi=[]
        , positional_args_usage='[database]'):
        """Build all the request a database arguments

        :param description: Help description
        :param required_args_single: A list of required db object types that will
            be filtered, defaults to []
        :param optional_args_single: A list of optional db object types that will
            be filtered for a single object, defaults to ['schema']
        :param optional_args_multi: A list of optional db object types that will
            be filtered for multiple objects, like: ['db', 'owner', 'table']
        :param positional_args_usage: positional args, defaults to '[database]'
        """
        if (
            ('schema' in optional_args_single)
            and (
                ('schema' in required_args_single)
                or ('schema' in optional_args_multi))):
            optional_args_single.remove('schema')

        self.args_process_init(description, positional_args_usage)

        self.args_add_positional_args()
        self.args_add_optional()
        self.args_add_connection_group()
        
        return db_args(
            required_args_single
            , optional_args_single
            , optional_args_multi
            , self)

    def args_process_init(self
        , description
        , positional_args_usage='[database]'
        , epilog=None):
        """Create an ArgumentParser object.

        :param description: Text to display before the argument help
        :param positional_args_usage: Description of how positional arguments
                                      are used
        """
        description_epilog = (
            '\n'
            '\noptional argument file/s:'
            '\n  @arg_file             file containing arguments')
        description= '%s%s' % (description, description_epilog)
        self.args_parser = argparse.ArgumentParser(
            description=description
            , usage="%%(prog)s %s[options]" % (
                positional_args_usage + ' '
                if positional_args_usage
                else '')
            , add_help=False
            , formatter_class=self.formatter
            , epilog=epilog
            , fromfile_prefix_chars='@')
        self.args_parser.convert_arg_line_to_args = convert_arg_line_to_args
        self.args_parser.positional_args_usage = positional_args_usage

    def args_add_positional_args(self):
        """Add positional arguments to the class's ArgumentParser object."""
        if self.args_parser.positional_args_usage:
            for arg in self.args_parser.positional_args_usage.split(' '):
                # Optional arguments are surrounded by square brackets
                trimmed_arg = arg.lstrip('[').rstrip(']')
                is_optional = len(trimmed_arg) != len(arg)

                if is_optional:
                    self.args_parser.add_argument(
                        trimmed_arg, nargs='?'
                        , help="optional %s to process" % trimmed_arg)
                else:
                    self.args_parser.add_argument(
                        trimmed_arg
                        , help="%s to process" % trimmed_arg)

    def args_add_connection_group(self):
        """Add conceptual grouping to improve the display of help messages.

        Creates a new group for arguments related to connection.
        """
        conn_grp = self.args_parser.add_argument_group('connection arguments')
        conn_grp.add_argument(
            "--host", "-h", "-H"
            , dest="host", help="specify database server hostname, "
                "overrides YBHOST env variable")
        conn_grp.add_argument(
            "--port", "-p", "-P"
            , dest="port", help="specify database server port, "
                "overrides YBPORT env variable, the default port is 5432")
        conn_grp.add_argument(
            "--dbuser", "-U"
            , dest="dbuser", help="specify database user, "
                "overrides YBUSER env variable")
        conn_grp.add_argument(
            "--conn_db", "-d", "-db", "-D"
            , dest="conn_db", help="specify database to connect to, "
                "overrides YBDATABASE env variable")
        conn_grp.add_argument(
            "--current_schema"
            , dest="current_schema"
            , help="specify the current schema after db connection")
        conn_grp.add_argument(
            "-W"
            , action="store_true"
            , help= "prompt for password instead of using the "
                "YBPASSWORD env variable")

    def args_add_optional(self):
        """Add conceptual grouping  to improve the display of help messages.

        Creates a new group for optional arguments.
        """
        self.args_parser.add_argument(
            "--help", "--usage", "-u"
            , action="help"
            , help="display this help message and exit")
        self.args_parser.add_argument(
            "--verbose"
            , type=int, default=0, choices=range(1, 4)
            , help="display verbose execution{1 - info, 2 - debug, "
                "3 - extended}")
        self.args_parser.add_argument(
            "--nocolor"
            , action="store_true"
            , help="turn off colored text output")
        self.args_parser.add_argument(
            "--version", "-v"
            , action="version", version=self.version
            , help="display the program version and exit")

    def args_process(self, has_conn_args = True):
        """Process arguments.

        Convert argument strings to objects and assign to the class. Then
        update the OS environment variables related to ybsql to match what was
        passed to this script. Finally, attempt to verify login credentials
        based on those variables.
        """
        self.args = self.args_parser.parse_args()

        if self.args.nocolor:
            text.nocolor = True

        # Get and set operating system environment variables related to ybsql
        if has_conn_args:
            if self.args.host:
                os.environ["YBHOST"] = self.args.host
            else:
                if os.environ.get("YBHOST") is None:
                    sys.stderr.write("%s: error: the host database server must "
                        "be set using the YBHOST environment variable or with "
                        "the argument: --host\n" % os.path.basename(sys.argv[0]))
                    exit(1)
                else:
                    self.args.host = os.environ.get("YBHOST")
            if self.args.port:
                os.environ["YBPORT"] = self.args.port
            if os.environ.get("YBPORT") is None:
                os.environ["YBPORT"] = '5432'  # default port
            if self.args.dbuser:
                os.environ["YBUSER"] = self.args.dbuser
            if self.args.conn_db:
                os.environ["YBDATABASE"] = self.args.conn_db

            self.login_verify()

    def login_verify(self):
        """Attempt to verify login credentials.

        Exits the program with a code if login fails.
        """
        if os.environ.get("YBUSER") is not None:
            if self.args.W or os.environ.get("YBPASSWORD") is None:
                os.environ["YBPASSWORD"] = getpass.getpass(
                    "Enter the password for user %s: "
                        % text.color(
                            os.environ.get("YBUSER")
                            , fg='cyan'))
        # We are missing YBUSER
        # Set an invalid password to skip the ybsql password prompt
        else:
            os.environ["YBPASSWORD"] = '-*-force bad password-*-'

        cmd_results = self.ybsql_query(
            """SELECT
    CURRENT_DATABASE() AS db
    , CURRENT_SCHEMA AS schema
    , SPLIT_PART(VERSION(), ' ', 4) AS version
    , SPLIT_PART(version, '-', 1) AS version_number
    , SPLIT_PART(version, '-', 2) AS version_release
    , SPLIT_PART(version_number, '.', 1) AS version_major
    , SPLIT_PART(version_number, '.', 2) AS version_minor
    , SPLIT_PART(version_number, '.', 3) AS version_patch""")
        db_info = cmd_results.stdout.split('|')
        if cmd_results.exit_code == 0:
            self.schema = db_info[1]
            # if --schema arg was set check if the schema is valid
            if len(self.schema) == 0:
                err = text.color(
                    'util: FATAL: schema "%s" does not exist\n'
                        % self.args.current_schema
                    , fg='red')
                sys.stderr.write(err)
                exit(2)
            if hasattr(self.args, 'database') and self.args.database:
                self.database = self.args.database
            else:
                self.database = db_info[0]
        else:
            sys.stderr.write(
                text.color(
                    cmd_results.stderr.replace('ybsql', 'util')
                    , fg='red'))
            exit(cmd_results.exit_code)

        self.ybdb_version = db_info[2]
        self.ybdb_version_number = db_info[3]
        self.ybdb_version_release = db_info[4]
        self.ybdb_version_major = int(db_info[5])
        self.ybdb_version_minor = int(db_info[6])
        self.ybdb_version_patch = int(db_info[7])
        self.ybdb_version_number_int = (
            self.ybdb_version_major * 10000
            + self.ybdb_version_minor * 100
            + self.ybdb_version_patch)

        if self.args.verbose >= 1:
            print(
                '%s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s'
                % (
                    text.color('Connecting to Host', style='bold')
                    , text.color(os.environ.get("YBHOST"), fg='cyan')
                    , text.color('Port', style='bold')
                    , text.color(os.environ.get("YBPORT"), fg='cyan')
                    , text.color('DB User', style='bold')
                    , text.color(os.environ.get("YBUSER"), fg='cyan')
                    , text.color('Database', style='bold')
                    , text.color(
                        '<user default>'
                            if os.environ.get("YBDATABASE") is None
                            else os.environ.get("YBDATABASE")
                        , fg='cyan')
                    , text.color('Current Schema', style='bold')
                    , text.color(
                        '<user default>'
                        if self.args.current_schema is None
                        else self.args.current_schema
                        , fg='cyan')
                    , text.color('YBDB', style='bold')
                    , text.color(self.ybdb_version, fg='cyan')))
        if self.args.verbose >= 2:
            print(
                'export YBHOST=%s;export YBPORT=%s;export YBUSER=%s%s'
                % (
                    os.environ.get("YBHOST")
                    , os.environ.get("YBPORT")
                    , os.environ.get("YBUSER")
                    , ''
                        if os.environ.get("YBDATABASE") is None
                        else
                            ';export YBDATABASE=%s'
                                % os.environ.get("YBDATABASE")))
    def call_cmd(self, cmd_str, stack_level=2):
        """Spawn a new process to execute the given command.

        Example: results = call_cmd('env | grep -i path')

        :param cmd_str: The string representing the command to execute
        :param stack_level: A number signifying the limit of stack trace
                            entries (Default value = 2)
        :return: The result produced by running the given command
        """
        if self.args.verbose >= 2:
            trace_line = traceback.extract_stack(None, stack_level)[0]
            print(
                '%s: %s, %s: %s, %s: %s\n%s\n%s'
                % (
                    text.color('--In file', style='bold')
                    , text.color(trace_line[0], 'cyan')
                    , text.color('Function', style='bold')
                    , text.color(trace_line[2], 'cyan')
                    , text.color('Line', style='bold')
                    , text.color(trace_line[1], 'cyan')
                    , text.color('--Executing--', style='bold')
                    , cmd_str))

        start_time = datetime.now()
        p = subprocess.Popen(
            cmd_str
            , stdout=subprocess.PIPE
            , stderr=subprocess.PIPE
            , shell=True)
        (stdout, stderr) = map(bytes.decode, p.communicate())
        end_time = datetime.now()

        results = cmd_results(p.returncode, stdout, stderr, self)

        if self.args.verbose >= 2:
            print(
                '%s: %s\n%s: %s\n%s\n%s%s\n%s'
                % (
                    text.color('--Execution duration', style='bold')
                    , text.color(end_time - start_time, fg='cyan')
                    , text.color('--Exit code', style='bold')
                    , text.color(
                        str(results.exit_code)
                        , fg=('red' if results.exit_code else 'cyan'))
                    , text.color('--Stdout--', style='bold')
                    , results.stdout.rstrip()
                    , text.color('--Stderr--', style='bold')
                    , text.color(results.stderr.rstrip(), fg='red')))

        return results

    def call_util_cmd(self, util_cmd, stack_level=2):
        """A wrapper for `call_cmd` used when running in verbose mode

        When verbose is turned on, the output of `call_cmd` will contain more
        info than exepected causing an execution error. To mitigate this issue,
        this wrapper calls `call_cmd` twice when verbose mode is turned on.
          - First time it produces the debug output
          - Second time it strips the verbose setting and returns the desired
            output without debug info

        :param util_cmd: The string representing the command to execute
        :param stack_level: A number signifying the limit of stack trace
                            entries (Default value = 2)
        :return: The result produced by running the given command
        """
        cmd_results = self.call_cmd(util_cmd)
        if self.args.verbose:
            util_cmd_wo_verbose = re.sub(
                r'(.*)--verbose [0-9](.*)', r'\1\2', util_cmd)
            cmd_results = self.call_cmd(util_cmd_wo_verbose, stack_level)

        return cmd_results

    def ybsql_query(self, sql_statement, options = '-A -q -t -v ON_ERROR_STOP=1 -X'):
        """Run and evaluate a query using ybsql.

        :param sql_statement: The SQL command string
        :options: ybsql command options
            default options
                -A: unaligned table output mode
                -q: run quietly (no messages, only query output)
                -t: print rows only
                -v: set ybsql variable NAME to VALUE
                    ON_ERROR_STOP: processing is stopped immediately,
                        with an exit code of 3
                -X: do not read startup file (~/.ybsqlrc)
        :return: The result produced by running the given command
        """
        if self.args.current_schema:
            sql_statement = "SET SCHEMA '%s';%s" % (
                self.args.current_schema, sql_statement)

        # default timeout is 75 seconds changing it to self.connect_timeout
        #   'host=<host>' string is required first to set connect timeout
        #   see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        ybsql_cmd = """ybsql %s "host=%s connect_timeout=%d" <<eof
%s
eof""" % (options, self.args.host, self.connect_timeout, sql_statement)

        cmd_results = self.call_cmd(ybsql_cmd, stack_level=3)

        return cmd_results

    def ts(self):
        """Get the current time (for time stamping)"""
        return str(datetime.now())

    def quote_object_path(self, object_paths):
        """Convert database object names to have double quotes where required"""
        quote_object_path = []
        for object_path in object_paths.split('\n'):
            objects = []
            for objct in object_path.split('.'):
                if len(re.sub('[a-z0-9_]', '', objct)) == 0:
                    objects.append(objct)
                else:
                    objects.append('"' + objct + '"')
            quote_object_path.append('.'.join(objects))

        return '\n'.join(quote_object_path)

    def call_stored_proc_as_anonymous_block(self
        , stored_proc
        , args={}
        , pre_sql=''
        , post_sql=''):
        """Convert an SQL stored procedure to an anonymous SQL block,
        then execute the anonymous SQL block.  This allows a user to run
        the stored procedure without building the procedure, lowering the
        barrier to run.

        :param stored_proc: The SQL stored_proc to be run stored in the sql directory
        :param args: a dictionary of input args/values to the stored_proc call
        :param pre_sql: SQL to execute before the stored_proc
        :param post_sql: SQL to execute after the stored_proc
        """
        return_marker = '>!>RETURN<!<:'

        try:
            filepath = os.path.split(__file__)[0] + ('/sql/%s.sql' % stored_proc)
            f = open(filepath, "r")
            stored_proc_sql = f.read()
            f.close()
        except FileNotFoundError:
            print("%s file does not exist..." % filepath)
            exit(2)

        regex = r"\s*CREATE\s*(OR\s*REPLACE)?\s*PROCEDURE\s*([a-z0-9_]+)\s*\((.*)\)\s*((RETURNS\s*([a-zA-Z]*).*))\s+LANGUAGE.+?(DECLARE\s*(.+))?RETURN\s*([^;]*);(.*)\$\$;"
        matches = re.search(regex, stored_proc_sql, re.IGNORECASE | re.DOTALL)

        if not matches:
            sys.stderr.write("Stored proc '%s' regex parse failed.\n" % stored_proc)
            exit(2)

        stored_proc_args          = matches.group(3)
        #TODO currently return_type only handles 1 word like; BOOLEAN
        stored_proc_return_type   = matches.group(6).upper()
        stored_proc_before_return = matches.group(8)
        stored_proc_return        = matches.group(9)
        stored_proc_after_return  = matches.group(10)

        anonymous_block = pre_sql + 'DO $$\nDECLARE\n    --arguments\n'
        if stored_proc_return_type not in ('BOOLEAN', 'BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
            sys.stderr.write('--unhandled proc return_type: %s\n'
                % stored_proc_return_type)
            exit(2)
 
        for arg in self.split(stored_proc_args):
            matches = re.search(r'(.*)\bDEFAULT\b(.*)'
                , arg, re.DOTALL | re.IGNORECASE)
            if matches:
                arg_def = matches.group(1).strip()
                default_value = matches.group(2).strip()
            else:
                arg_def = arg.strip()
                default_value = None

            matches = re.search(r'([a-zA-Z0-9_]+)\b\s*([ a-zA-z]+)(.*)'
                , arg_def, re.DOTALL | re.IGNORECASE)
            arg_name = matches.group(1).strip()
            arg_type = matches.group(2).strip()
            arg_type_size = matches.group(3).strip()

            #print('arg: %s, dt: %s, dts: %s, default: %s' % (arg, arg_datatype, arg_datatype_size, default))
            if arg_name in args:
                if arg_type == 'VARCHAR':
                    anonymous_block += ("    %s %s%s = $A$%s$A$;\n"
                        % (arg_name, arg_type, arg_type_size, args[arg_name]))
                elif arg_type in ('BOOLEAN', 'BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
                    anonymous_block += ("    %s %s = %s;\n"
                        % (arg_name, arg_type, args[arg_name]))
                else:
                    sys.stderr.write("Unhandled proc arg_type: %s\n"
                        % (arg_type))
                    exit(2)
            elif default_value:
                anonymous_block += ("    %s %s = %s;\n"
                    % (arg_name, arg_type, default_value))
            else:
                sys.stderr.write("Missing proc arg: %s for proc: %s\n"
                    % (arg_name, stored_proc))
                exit(2)

        anonymous_block += ("    --variables\n    %sRAISE INFO '%s%%', %s;%s$$;%s"
            % (
                stored_proc_before_return, return_marker, stored_proc_return
                , stored_proc_after_return, post_sql))

        anonymous_block = re.sub(r'\$([a-zA-Z0-9]*)\$', r'\$\1\$'
            , anonymous_block)

        cmd_results = self.ybsql_query(anonymous_block)

        # pg/plsql RAISE INFO commands are sent to stderr.  The following moves
        #   the RAISE INFO data to be returned as stdout. 
        if cmd_results.stderr.strip() != '':
            return_value = None
            stderr = ''
            stdout = cmd_results.stdout
            for line in cmd_results.stderr.split('\n'):
                if line[0:7] == 'INFO:  ':
                    if line[0:20] == 'INFO:  >!>RETURN<!<:':
                        return_value = line[20:].strip()
                    else:
                        stdout += line[7:] + '\n'
                else:
                    stderr += line

            if not return_value:
                sys.stderr.write(cmd_results.stderr)
                exit(2)

            cmd_results.stderr = stderr
            cmd_results.stdout = stdout

            if stored_proc_return_type == 'BOOLEAN':
                boolean_values = {'t': True, 'f': False, '<NULL>': None}
                cmd_results.proc_return = boolean_values.get(
                    return_value, None)
            elif stored_proc_return_type in ('BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
                cmd_results.proc_return = (
                    None
                    if return_value == '<NULL>'
                    else int(return_value))
            else:
                sys.stderr.write("Unhandled proc return_type: %s\n"
                    % (stored_proc_return_type))
                exit(2)

        return cmd_results

    def split(self, str, delim=','):
        #todo handle escape characters
        open_close_char = {"'":"'", '"':'"', '(':')', '[':']', '{':'}'}
        close_char = []
        for char in open_close_char.keys():
            close_char.append(open_close_char[char])
        open_and_close_char = []
        for char in open_close_char.keys():
            if char == open_close_char[char]:
                open_and_close_char.append(char)

        open_char = []
        token = ''
        tokens = []
        for i in range(len(str)):
            if len(open_char) == 0 and str[i] == delim:
                tokens.append(token.strip())
                token = ''
            else:
                token += str[i]

            if str[i] in open_close_char.keys():
                if (str[i] in open_and_close_char
                    and len(open_char) > 0
                    and open_char[-1] == str[i]):
                    None
                else:
                    skip_close = True
                    open_char.append(str[i])

            if str[i] in close_char and not skip_close:
                if (len(open_char) == 0
                    or open_close_char[open_char[-1]] != str[i]):
                    sys.stderr.write('Invalid Argument List: %s'
                        % str)
                    exit(1)
                else:
                    open_char.pop()
            else:
                skip_close = False

        if len(open_char) > 0:
            sys.stderr.write('Invalid Argument List: %s\n' % str)
            exit(1)
        else:
            tokens.append(token.strip())

        return tokens

class intRange:
    """Custom argparse type representing a bounded int
    """

    def __init__(self, imin=None, imax=None):
        self.imin = imin
        self.imax = imax

    def __call__(self, arg):
        try:
            value = int(arg)
        except ValueError:
            raise self.exception()
        if ((self.imin is not None and value < self.imin)
            or (self.imax is not None and value > self.imax)):
            raise self.exception()
        return value

    def exception(self):
        if self.imin is not None and self.imax is not None:
            return argparse.ArgumentTypeError(
                "Must be an integer in the range [{}, {}]"
                    .format(self.imin, self.imax))
        elif self.imin is not None:
            return argparse.ArgumentTypeError(
                "Must be an integer >= {}"
                    .format(self.imin))
        elif self.imax is not None:
            return argparse.ArgumentTypeError(
                "Must be an integer <= {}"
                    .format(self.imax))
        else:
            return argparse.ArgumentTypeError(
                "Must be an integer")


class cmd_results:

    def __init__(self, exit_code, stdout, stderr, common):
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.common = common

    def write(self, head='', tail='', quote=False):
        sys.stdout.write(head)
        if self.stdout != '':
            sys.stdout.write(
                self.common.quote_object_path(self.stdout)
                if quote
                else self.stdout)
        if self.stderr != '':
            sys.stdout.write(text.color(self.stderr, fg='red'))
        else:
            sys.stdout.write(tail)


class db_args:
    """Class that handles database objects that are used as a filter 
    """

    def __init__(self
        , required_args_single
        , optional_args_single
        , optional_args_multi
        , common):
        """During init the command line filter arguments are built for the
        requested object_types

        :param required_args_single: A list of required db object types that will
            be filtered, like: ['db', 'owner', 'table']
        :param optional_args_single: A list of optional db object types that will
            be filtered for a single object, like: ['db', 'owner', 'table']
        :param optional_args_multi: A list of optional db object types that will
            be filtered for multiple objects, like: ['db', 'owner', 'table']
        :param common: the common object created by the caller, this is needed
            to get a handle for common.argparser and common.args
        """
        self.required_args_single = required_args_single
        self.optional_args_single = optional_args_single
        self.optional_args_multi = optional_args_multi
        self.schema_is_required = False
        self.common = common

        if len(self.required_args_single):
            args_filter_grp = (
                self.common.args_parser.add_argument_group(
                    'required database object filter arguments'))
            for otype in self.required_args_single:
                self.args_add_object_type_single(
                    otype, args_filter_grp, True)

        if (len(self.optional_args_single)
            or len(self.optional_args_multi)):
            args_filter_grp = (
                self.common.args_parser.add_argument_group(
                    'optional database object filter arguments'))
            for otype in self.optional_args_single:
                self.args_add_object_type_single(
                    otype, args_filter_grp, False)
            for otype in self.optional_args_multi:
                self.args_add_optional_args_multi(
                    otype, args_filter_grp)

    def args_add_object_type_single(self
        , otype
        , filter_grp
        , is_required):
        """Add object type filter arguments that only have a single entry
        to the filter argument group.

        :param otype: A database object that will be filtered
        :filter_grp: group the new filter is added to
        """
        default_help = ''
        if otype == 'schema':
            self.schema_is_required = is_required
            if not is_required:
                default_help = ', defaults to CURRENT_SCHEMA'

        filter_grp.add_argument(
            "--%s" % otype
            , dest="%s" % otype
            , required=is_required
            , metavar="%s_NAME" % otype.upper()
            , help=("%s name%s" % (otype, default_help)))

    def args_add_optional_args_multi(self, otype, filter_grp):
        """Add optional object type filter arguments to the filter argument group.

        :param otype: A database object that will be filtered
        :filter_grp: group the new filter is added to
        """
        notation_help = ''
        if (otype in ['db', 'database', 'schema', 'table', 'column'
            , 'view', 'sequence', 'object', 'owner']):
            notation_help = (""", use '"Name"' notation """
                "for case dependent names")
        filter_grp.add_argument(
            "--%s_in" % otype
            , dest="%s_in_list" % otype
            , nargs="+", action='append', metavar="%s_NAME" % otype.upper(),
            help="%s/s in the list%s" % (otype, notation_help))
        filter_grp.add_argument(
            "--%s_NOTin" % otype
            , dest="%s_not_in_list" % otype
            , nargs="+", action='append', metavar="%s_NAME" % otype.upper()
            , help="%s/s NOT in the list%s" % (otype, notation_help))
        filter_grp.add_argument(
            "--%s_like" % otype
            , dest="%s_like_pattern" % otype
            , nargs="+", action='append', metavar="PATTERN"
            , help="%s/s like the pattern/s" % otype)
        filter_grp.add_argument(
            "--%s_NOTlike" % otype
            , dest="%s_not_like_pattern" % otype
            , nargs="+", action='append', metavar="PATTERN",
            help="%s/s NOT like the pattern/s" % otype)        

    def has_optional_args_single_set(self, otype):
        """Has an optional filter been set for the requested object type.

        :param typ: database object type to check
        """
        ret_value = False
        
        if otype in self.optional_args_single:
            ret_value = eval('self.common.args.%s' % otype)
        
        return ret_value

    def has_optional_args_multi_set(self, otype):
        """Has an optional filter been set for the requested object type.

        :param typ: database object type to check
        """
        ret_value = False
        
        if otype in self.optional_args_multi:
            (arg_in_list, arg_like_pattern, arg_not_in_list
                , arg_not_like_pattern) = (
                self.get_optional_args_multi(otype))
            ret_value = (arg_in_list or arg_like_pattern
                or arg_not_in_list or arg_not_like_pattern)
        
        return ret_value

    def get_optional_args_multi(self, otype):
        """Get the set of 4 filter arguments for the requested object type.

        :param typ: database object type to get filters for
        """
        arg_in_list = eval('self.common.args.%s_in_list' % otype)
        if arg_in_list:
            arg_in_list = sorted(set(sum(arg_in_list, [])))
        arg_like_pattern = eval('self.common.args.%s_like_pattern' % otype)
        if arg_like_pattern:
            arg_like_pattern = sorted(set(sum(arg_like_pattern, [])))
        arg_not_in_list = eval('self.common.args.%s_not_in_list' % otype)
        if arg_not_in_list:
            arg_not_in_list = sorted(set(sum(arg_not_in_list, [])))
        arg_not_like_pattern = eval(
            'self.common.args.%s_not_like_pattern' % otype)
        if arg_not_like_pattern:
            arg_not_like_pattern = sorted(
                set(sum(arg_not_like_pattern, [])))

        return (
            arg_in_list
            , arg_like_pattern
            , arg_not_in_list
            , arg_not_like_pattern)

    def schema_set_all_if_none(self):
        if not self.has_optional_args_multi_set('schema'):
            self.common.args.schema_like_pattern = [['%']]

    def build_sql_filter(self
        , object_column_names
        , indent='    '
        , escape_quotes=False):
        """Build the SQL filter clause for requested dictionary of object types.

        :param object_column_names: dictionary that maps object type to column
            name to be used in SQL clause like; {'db':'db_name', 'owner':'owner_name'}
        :param indent: indet used after cariiage return for SQL creation
            (Default value = '    ')
        :param escape_quotes: changes all single quotes in return value form "'"
            to "''" when set to True (Default value = False)
        :returns: SQL filter clause
        """
        and_objects=[]
        for otype in self.required_args_single:
            if otype in object_column_names.keys():
                and_objects.append(
                    self.build_args_single_sql_filter(
                        otype, object_column_names[otype]
                        , indent, escape_quotes))
        for otype in self.optional_args_single:
            if otype in object_column_names.keys():
                and_objects.append(
                    self.build_args_single_sql_filter(
                        otype, object_column_names[otype]
                        , indent, escape_quotes))
        for otype in self.optional_args_multi:
            if otype in object_column_names.keys():
                and_objects.append(
                    self.build_optional_args_multi_sql_filter(
                        otype, object_column_names[otype]
                        , indent, escape_quotes))

        #special handling for CURRENT_SCHEMA
        if ('schema' in object_column_names.keys()
            and not self.has_optional_args_multi_set('schema')
            and not (
                ('schema' in self.required_args_single
                    or 'schema' in self.optional_args_single)
                and self.common.args.schema != None)):
            and_objects.append('%s = CURRENT_SCHEMA'
                % object_column_names['schema'])

        filter_clause = ('\n' + indent + 'AND ').join(and_objects)

        return ('TRUE' if filter_clause == '' else filter_clause)

    def build_args_single_sql_filter(self
        , otype, column_name, indent='', escape_quotes=False):
        """Build the SQL filter clause for a required object type.

        :param otype: object type to build the SQL filter clause
        :param column_name: the SQL column name to use in the SQL for the
            object type
        :param indent: indet used after cariiage return for SQL creation
            (Default value = '')
        :param escape_quotes: changes all single quotes in return value form "'"
            to "''" when set to True (Default value = False)
        :returns: SQL filter clause
        """
        arg_value = eval('self.common.args.%s' % otype)
        if arg_value:
            filter_clause = ("%s = '%s'" % (column_name, arg_value))
        else:
            filter_clause = 'TRUE'

        return filter_clause

    def build_optional_args_multi_sql_filter(self
        , otype, column_name, indent='', escape_quotes=False):
        """Build the SQL filter clause for an optional object type.

        :param otype: object type to build the SQL filter clause
        :param column_name: the SQL column name to use in the SQL for the
            object type
        :param indent: indet used after cariiage return for SQL creation
            (Default value = '')
        :param escape_quotes: changes all single quotes in return value form "'"
            to "''" when set to True (Default value = False)
        :returns: SQL filter clause
        """
        or_objects = []
        and_objects = []

        arg_in_list, arg_like_pattern, arg_not_in_list, arg_not_like_pattern = (
            self.get_optional_args_multi(otype))

        if arg_in_list:
            objects = []
            for name in arg_in_list:
                if name[0] == '"':
                    objects.append(name.replace('"', "'"))
                else:
                    objects.append("'%s'" % name.lower())
            or_objects.append(
                '<column_name> IN (%s)' % ', '.join(objects))

        if arg_like_pattern:
            for pattern in arg_like_pattern:
                or_objects.append(
                    #"LOWER(<column_name>) LIKE LOWER('%s')" % pattern)
                    "<column_name> LIKE '%s'" % pattern)

        if len(or_objects) > 0:
            and_objects.append('(%s)' % ' OR '.join(or_objects))

        if arg_not_in_list:
            objects = []
            for name in arg_not_in_list:
                if name[0] == '"':
                    objects.append(name.replace('"', "'"))
                else:
                    objects.append("'%s'" % name.lower())
            and_objects.append(
                '<column_name> NOT IN (%s)' % ', '.join(objects))

        if arg_not_like_pattern:
            for pattern in arg_not_like_pattern:
                and_objects.append(
                    #"LOWER(<column_name>) NOT LIKE LOWER('%s')\n" %
                    #pattern)
                    "<column_name> NOT LIKE '%s'" % pattern)

        filter_clause = ('\n' + indent + 'AND ').join(and_objects)
        filter_clause = filter_clause.replace('<column_name>', column_name)

        if escape_quotes:
            filter_clause = filter_clause.replace("'", "''")

        return ('TRUE' if filter_clause == '' else filter_clause)

class text:
    colors = {
        'black': 0
        , 'red': 1
        , 'green': 2
        , 'yellow': 3
        , 'blue': 4
        , 'purple': 5
        , 'cyan': 6
        , 'white': 7
    }

    styles = {
        'no_effect': 0
        , 'bold': 1
        , 'underline': 2
        , 'italic': 3
        , 'negative2': 5
    }

    nocolor = False

    @staticmethod
    def color_str(fg='white', bg='black', style='no_effect'):
        """Return a formatted string.

        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string formatted with color and style
        """
        return '\033[%d;%d;%dm' % (
            text.styles[style.lower()]
            , 30 + text.colors[fg.lower()]
            , 40 + text.colors[bg.lower()])

    @staticmethod
    def color(txt, fg='white', bg='black', style='no_effect'):
        """Style a given string with color.

        :param txt: The text input string
        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string with added color
        """
        colored_text = '%s%s%s' % (
            text.color_str(fg, bg, style), txt, text.color_str())
        return txt if text.nocolor else colored_text

def convert_arg_line_to_args(line):
    for arg in shlex.split(line):
        if not arg.strip():
            continue
        if arg[0] == '#':
            break
        yield arg

# Standalone tests
# Example: yb_common.py -h YB14 -U denav -D denav --verbose 3
if __name__ == "__main__":
    common = common('Common Standalone Debug Run', 'object')
    common.args_add_positional_args()
    common.args_add_optional()
    common.args_add_connection_group()
    common.args_add_filter_group()
    common.args_process()

    if common.args.verbose >= 3:
        # Print extended information on the environment running this program
        print('--->%s\n%s' % ("(common.call_cmd('lscpu')).stdout",
                              common.call_cmd('lscpu').stdout))
        print('--->%s\n%s' % ("platform.platform()", platform.platform()))
        print('--->%s\n%s' % ("platform.python_implementation()",
                              platform.python_implementation()))
        print('--->%s\n%s' % ("sys.version", sys.version))
        print('--->%s\n%s' % ("common.args", common.args))
        print('--->%s\n%s' % ("common.filter_clause", common.filter_clause))
        print('--->%s\n%s' % ("common.util_dir_path", common.util_dir_path))
        print('--->%s\n%s' % ("common.util_file_name", common.util_file_name))
        print('--->%s\n%s' % ("common.util_name", common.util_name))
        print('--->%s\n%s' % ("common.version", common.version))
