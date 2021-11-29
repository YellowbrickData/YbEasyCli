#!/usr/bin/env python3
"""Performs functions such as argument parsing, login verification, logging,
and command execution that are common to all utilities in this package.
"""

import argparse
import base64
import copy
import csv
import getpass
import gzip
import os
import platform
import pprint
import re
import random
import signal
import shlex
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import datetime, date
from glob import glob
from tabulate import tabulate

# Provides gracefule error when user issues a CTRL-C to break out of a yb_<util>
#    TODO doesn't work well in powershell
def signal_handler(signal, frame):
    Common.error('user terminated...')
signal.signal(signal.SIGINT, signal_handler)

class Common:
    version = '20211129'
    verbose = 0

    util_dir_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    util_file_name = os.path.basename(os.path.realpath(sys.argv[0]))
    util_name = util_file_name.split('.')[0]
    start_ts = datetime.now()
    is_windows = platform.system() == 'Windows'
    is_cygwin = sys.platform == 'cygwin'

    if not is_windows:
        # supresses Linux error thrown when using pipe; like: 'yb_<util> | head -10'
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    @staticmethod
    def error(msg, exit_code=1, color='red', no_exit=False):
        if Common.verbose >= 3:
            traceback.print_stack()
        sys.stderr.write("%s: %s\n" % (
            Text.color(Common.util_file_name, style='bold')
            , Text.color(msg, color)))
        if not no_exit:
            exit(exit_code)

    @staticmethod
    def read_file(file_path, on_read_error_exit=True, color='red'):
        if Common.verbose >= 3:
            print('%s: %s' % (Text.color('--Reading file', style='bold'), file_path) )
        data = None
        try:
            with open(file_path) as f:
                data = f.read()
                f.close()
        except IOError as ioe:
            if on_read_error_exit:
                Common.error(ioe)

        return data

    @staticmethod
    def ts(self):
        """Get the current time (for time stamping)"""
        return str(datetime.now())

    @staticmethod
    def get_uid():
        return '%s_%05d' % (datetime.now().strftime('%Y%m%d_%H%M%S'), random.randint(0, 99999))

    @staticmethod
    def str_to_gzip_b64(string):
        cmdline_args_gz = gzip.compress(bytes(string, 'UTF-8'))
        return base64.b64encode(cmdline_args_gz).decode()

    @staticmethod
    def gzip_b64_to_str(gzip_b64_str):
        gz = base64.b64decode(gzip_b64_str)
        return gzip.decompress(gz).decode()

    @staticmethod
    def split_db_object_name(object_name):
        """Break a fully or partially qualified DB object name into parts"""
        name_list = object_name.split('.')
        name_list.reverse()
        table = name_list[0]
        schema = name_list[1] if len(name_list) > 1 else None
        database = name_list[2] if len(name_list) > 2 else None
        return database, schema, table

    @staticmethod
    def quote_object_paths(object_paths, quote_all=False):
        """Convert database object names to have double quotes where required
        quote_all: will quote all names even SQL object names that don't require quotes
        """
        quote_object_paths = []
        #for object_path in object_paths.split('\n'):
        for object_path in re.split('\r\n|\n', object_paths):
            #first remove all double quotes to start with an unquoted object path
            #   sometimes the incoming path is partially quoted
            object_path = object_path.replace('"', '')
            objects = []
            for objct in object_path.split('.'):
                if len(re.sub('[a-z0-9_]', '', objct)) == 0 and not quote_all:
                    objects.append(objct)
                else:
                    objects.append('"' + objct + '"')
            quote_object_paths.append('.'.join(objects))

        return '\n'.join(quote_object_paths)

    @staticmethod
    def qa(object_paths):
        if type(object_paths) == list:
            new_list = []
            for object_path in object_paths:
                new_list.append(Common.quote_object_paths(object_path, quote_all=True))
            return new_list
        else:
            return Common.quote_object_paths(object_paths, quote_all=True)

    @staticmethod
    def split(str, delim=','):
        """split which handles embedded delims
        within single/double quotes, parens, brakets, and curley braces"""
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
        if len(str):
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
                        Common.error('Invalid Argument List: %s' % str)
                    else:
                        open_char.pop()
                else:
                    skip_close = False

            if len(open_char) > 0:
                Common.error('Invalid Argument List: %s' % str)
            else:
                tokens.append(token.strip())

        return tokens

class Cmd:
    cmd_ct = 0
    def __init__(self, cmd_str, escape_dollar=True, stack_level=2, wait=True, stdin=None):
        """Spawn a new process to execute the given command.

        Example: cmd = Cmd('env | grep -i path')

        :param cmd_str: string, representing the command to execute
        :param escape_dollar: boolean, places a back slash before each $ in the cmd
        :param stack_level: number, used in verbose mode to print where in the python
                            code this Cmd was called
        :param wait: boolean, wait on the cmd results
        """
        Cmd.cmd_ct += 1
        self.cmd_id = Cmd.cmd_ct

        if Common.is_windows:
            cmd_str = self.windows_pre_cmd(cmd_str)

        if Common.verbose >= 2:
            trace_line = traceback.extract_stack(None, stack_level)[0]
            print(
                '%s: %s, %s: %s, %s: %s\n%s\n%s'
                % (
                    Text.color('--In file', style='bold')
                    , Text.color(trace_line[0], 'cyan')
                    , Text.color('Function', style='bold')
                    , Text.color(trace_line[2], 'cyan')
                    , Text.color('Line', style='bold')
                    , Text.color(trace_line[1], 'cyan')
                    , Text.color('--Cmd Id(%d) Executing--' % self.cmd_id, style='bold')
                    , cmd_str))
        elif Common.verbose >= 1:
            print('%s: %s'
                % (Text.color('Executing', style='bold'), cmd_str))

        if escape_dollar and not(Common.is_windows):
            cmd_str = cmd_str.replace('$','\$')

        self.cmd_dtr = cmd_str
        self.start_time = datetime.now()

        self.p = subprocess.Popen(
            cmd_str
            , stdin=subprocess.PIPE
            , stdout=subprocess.PIPE
            , stderr=subprocess.PIPE
            , shell=not(Common.is_windows))

        if stdin:
            self.p.stdin.write(stdin.encode('utf-8'))
            self.p.communicate()[0]

        if wait:
            self.wait()

        if Common.is_windows:
            cmd_str = self.windows_post_cmd()

    def windows_pre_cmd(self, cmd_str):
        self.prefix = ".YbEasyCli_Cmd_"
        fd, self.tmp_ps1_file = tempfile.mkstemp(prefix=self.prefix, suffix=".ps1")
        os.close(fd)

        ps1_str = cmd_str
        fd = open(self.tmp_ps1_file, 'w')
        fd.write(ps1_str)
        fd.close()

        if Common.verbose >= 2:
            print('%s %s\n%s'
                % (
                    Text.color('--File', style='bold')
                    , Text.color(self.tmp_ps1_file, 'cyan'), ps1_str ) )

        return "powershell -ExecutionPolicy ByPass -Noninteractive -NoLogo -NoProfile -File %s" % self.tmp_ps1_file

    def windows_post_cmd(self):
        #clean up old tmp files 
        glob_str = self.tmp_ps1_file[0:(self.tmp_ps1_file.find(self.prefix) + len(self.prefix))] + '*'
        threshold_time = time.time() - (60*360)
        for tmp_ps1_old_file in glob(glob_str):
            try:
                creation_time = os.stat(tmp_ps1_old_file).st_ctime
                if creation_time < threshold_time and self.tmp_ps1_file.replace('\\\\', '\\') != tmp_ps1_old_file:
                    os.remove(tmp_ps1_old_file)
            except:
                None

    def wait(self):
        #(stdout, stderr) = map(bytes.decode, p.communicate())
        #TODO change the decode to reflect coding used in the DB connection
        (stdout, stderr) = self.p.communicate()
        self.exit_code = self.p.returncode
        self.stdout = stdout.decode("utf-8", errors='ignore')
        self.stderr = stderr.decode("utf-8", errors='ignore')

        end_time = datetime.now()

        if Common.verbose >= 2:
            print(
                '%s: %s\n%s: %s\n%s\n%s%s\n%s'
                % (
                    Text.color('--Cmd Id(%d) Execution duration ' % self.cmd_id, style='bold')
                    , Text.color(end_time - self.start_time, fg='cyan')
                    , Text.color('--Exit code', style='bold')
                    , Text.color(
                        str(self.exit_code)
                        , fg=('red' if self.exit_code else 'cyan'))
                    , Text.color('--Stdout--', style='bold')
                    , self.stdout.rstrip()
                    , Text.color('--Stderr--', style='bold')
                    , Text.color(self.stderr.rstrip(), fg='red')))

    def write(self, head='', tail='', quote=False):
        sys.stdout.write(head)
        if self.stdout != '':
            sys.stdout.write(
                Common.quote_object_paths(self.stdout)
                if quote
                else self.stdout)
        if self.stderr != '':
            Common.error(self.stderr, no_exit=True)
        sys.stdout.write(tail)

    def on_error_exit(self, write=True, head='', tail=''):
        if self.stderr != '' or self.exit_code != 0:
            if write:
                self.write(head,tail)
            exit(self.exit_code)

class ArgsHandler:
    """This class contains functions used for argument parsing
    """
    def __init__(self, config, init_default=True):
        self.config = config
        if init_default:
            self.init_default()

    def init_default(self):
        """Build all the requested database arguments
        """
        required_args_single = self.config['required_args_single']
        optional_args_single = self.config['optional_args_single']
        optional_args_multi = self.config['optional_args_multi']

        if (
            ('schema' in optional_args_single)
            and (
                ('schema' in required_args_single)
                or ('schema' in optional_args_multi))):
            optional_args_single.remove('schema')

        self.args_process_init()

        self.args_add_positional_args() #TODO unused, may remove in the future
        self.args_add_optional()
        self.args_add_connection_group()

        self.config['additional_args']()

        if self.config['output_tmplt_default']:
            self.add_output_args()

        if self.config['report_columns'] is not None:
            self.add_report_args()

        self.db_filter_args = DBFilterArgs(
            required_args_single
            , optional_args_single
            , optional_args_multi
            , self)

    def formatter(self, prog):
        return argparse.RawDescriptionHelpFormatter(prog, width=100)

    def args_process_init(self, epilog=None):
        """Create an ArgumentParser object.

        :param description: Text to display before the argument help
        :param positional_args_usage: Description of how positional arguments
                                      are used
        """
        description = self.config['description']
        positional_args_usage = self.config['positional_args_usage']

        description_epilog = (
            '\n'
            '\noptional argument file/s:'
            '\n  @arg_file             file containing arguments'
            '\n                        to enter multi-line argument, use: --arg """multi-line value"""')
        description= '%s%s' % (description, description_epilog)

        usage_example = self.args_usage_example()
        if usage_example:
            epilog = '%s%s' % ((epilog if epilog else ''), usage_example)

        self.args_parser = UtilArgParser(
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

    def args_add_connection_group(self, type=None, type_desc=''):
        """Add conceptual grouping to improve the display of help messages.

        Creates a new group for arguments related to connection.
        """
        if not type:
            conn_grp = self.args_parser.add_argument_group(
                'connection arguments')
            conn_grp.add_argument(
                "--host", "-h", "-H"
                , dest="host", help="database server hostname, "
                    "overrides YBHOST env variable")
            conn_grp.add_argument(
                "--port", "-p", "-P"
                , dest="port", help="database server port, "
                    "overrides YBPORT env variable, the default port is 5432")
            conn_grp.add_argument(
                "--dbuser", "-U"
                , dest="dbuser", help="database user, "
                    "overrides YBUSER env variable")
            conn_grp.add_argument(
                "--conn_db", "--db", "-d", "-D"
                , dest="conn_db", help="database to connect to, "
                    "overrides YBDATABASE env variable")
            conn_grp.add_argument(
                "--current_schema"
                , help="current schema after db connection")
            conn_grp.add_argument(
                "-W"
                , action="store_true"
                , help= "prompt for password instead of using the "
                    "YBPASSWORD env variable")
        else:
            conn_grp = self.args_parser.add_argument_group(
                'connection %s arguments' % type_desc)
            conn_grp.add_argument(
                "--%s_host" % type
                , help="%s database server hostname, "
                    "overrides YBHOST env variable" % type_desc)
            conn_grp.add_argument(
                "--%s_port" % type
                , help="%s database server port, overrides YBPORT "
                    "env variable, the default port is 5432" % type_desc)
            conn_grp.add_argument(
                "--%s_dbuser" % type
                , help="%s database user, "
                    "overrides YBUSER env variable" % type_desc)
            conn_grp.add_argument(
                "--%s_conn_db" % type
                , help="%s database to connect to, "
                    "overrides YBDATABASE env variable" % type_desc)
            conn_grp.add_argument(
                "--%s_current_schema" % type
                , help="current schema after db connection")
            conn_grp.add_argument(
                "--%s_W" % type
                , action="store_true"
                , help= "prompt for password instead of using the "
                    "YBPASSWORD env variable")

        return conn_grp

    def args_add_optional(self):
        """Add conceptual grouping  to improve the display of help messages.

        Creates a new group for optional arguments.
        """
        self.args_parser.add_argument(
            "--help", "--usage", "-u", action="help"
            , help="display this help message and exit")
        self.args_parser.add_argument(
            "--verbose", type=int, default=0, choices=range(1, 4)
            , help="display verbose execution{1 - info, 2 - debug, 3 - extended}")
        self.args_parser.add_argument(
            "--nocolor", action="store_true", help="turn off colored text output")
        self.args_parser.add_argument(
            "--version", "-v", action="version", version=Common.version
            , help="display the program version and exit")

    def add_report_args(self):
        args_optional_grp = self.args_parser.add_argument_group('optional report arguments')

        args_optional_grp.add_argument("--report_type"
            , choices=['formatted', 'psv', 'ctas', 'insert'], default='formatted'
            , help=("formatted: output a formatted report, psv: output pipe seperated row data,"
                " ctas: create a table containing the report data,"
                " insert: insert report data into an existing table, defaults to formatted") )
        args_optional_grp.add_argument('--report_delimiter', help=argparse.SUPPRESS, default='|')
        args_optional_grp.add_argument("--report_dst_table", metavar='table'
            , help="report destination table applies to report_type 'ctas' and 'insert' only")

        columns_help_extra = (
            (', available report columns: %s' % self.config['report_columns'].replace('|', ', ') )
            if self.config['report_columns'] != 'get_post_db_conn'
            else '')
        args_optional_grp.add_argument("--report_include_columns"
            , nargs='+', metavar='column'
            , help=("limit the report to the list of column names, the report will be created in the"
                " column order supplied%s" % columns_help_extra ) )
        args_optional_grp.add_argument("--report_exclude_columns"
            , nargs='+', metavar='column'
            , help="list of column names to exclude from the report")

        default_order = (self.config['report_default_order'].split('|')
            if self.config['report_default_order'] != [] else [])
        default_order_str = ((', defaults to: ' + ' '.join(default_order) )
            if len(default_order) > 0 else '')
        args_optional_grp.add_argument(
            "--report_order_by", nargs="+", metavar='column_name <ASC|DESC>', default=default_order
            , help=("report order by columns%s" % default_order_str ) )

        args_optional_grp.add_argument(
            "--report_add_ts_column", action="store_true", help=("add first column with current timestamp to the report" ) )

    def add_output_args(self):
        args_optional_grp = self.args_parser.add_argument_group(
            'optional output arguments')

        args_optional_grp.add_argument(
            "--output_template", metavar='template', dest='template'
            , help="template used to print output"
                ", defaults to '%s'"
                ", template variables include; %s"
                    % (self.config['output_tmplt_default']
                        , '{' + '}, {'.join(self.config['output_tmplt_vars']) + '}' )
            , default=self.config['output_tmplt_default'])
        args_optional_grp.add_argument(
            "--exec_output", action="store_true"
            , help="execute output as SQL, defaults to FALSE")

    def args_usage_example(self):
        usage = self.config['usage_example']
        if len(usage):
            extra = self.config['usage_example_extra']
            if 'cmd_line_args' in extra:
                usage['cmd_line_args'] += ' ' + extra['cmd_line_args']
            if 'file_args' in extra:
                usage['file_args'].extend(extra['file_args'])

            text = ('example usage:'
                + '\n  %s %s' % (Common.util_file_name, usage['cmd_line_args']))

            if Common.is_windows:
                # in Windows the file argument @file_name needs to be placed in single quotes
                text = re.sub(r'(\s)(\@[^\s]*)', r"\1'\2'", text)
                text = re.sub(r'([^\s]+\.py)', r"python \1", text)

            if 'file_args' in usage.keys():
                for file_dict in usage['file_args']:
                    for file in file_dict.keys():
                        text = text + "\n\n  file '%s' contains:" % file
                        for line in file_dict[file].split('\n'):
                            text =  text + '\n    ' + line
        else:
            text = None

        return(text)

    def process_report_args(self):
        if self.config['report_columns'] != 'get_post_db_conn':
            if (self.args.report_include_columns and self.args.report_exclude_columns):
                self.args_parser.error('only --report_include_columns or --report_exclude_columns may be defined but not both')

            report_columns = self.config['report_columns'].split('|')
            if self.args.report_include_columns:
                self.args.report_include_columns = re.sub(r'\s+', '|', ' '.join(self.args.report_include_columns).strip()).split('|')
                for column in self.args.report_include_columns:
                    if column not in report_columns:
                        self.args_parser.error("include column '%s' is not one of the report columns %s"
                            % (column, pprint.PrettyPrinter().pformat(report_columns)) )
                self.config['report_columns'] = self.args.report_include_columns
            elif self.args.report_exclude_columns:
                self.args.report_exclude_columns = re.sub(r'\s+', '|', ' '.join(self.args.report_exclude_columns).strip()).split('|')
                for column in self.args.report_exclude_columns:
                    if column not in report_columns:
                        self.args_parser.error("exclude column '%s' is not one of the report columns %s"
                            % (column, pprint.PrettyPrinter().pformat(report_columns)) )
                for column in self.args.report_exclude_columns:
                    report_columns.remove(column)
                self.config['report_columns'] = report_columns
            else:
                self.config['report_columns'] = report_columns

            if ((self.args.report_dst_table and self.args.report_type not in ['ctas', 'insert'])
                or (self.args.report_type in ['ctas', 'insert']) and not(self.args.report_dst_table)):
                self.args_parser.error("both --report_dst_table and --report_type must be set for --report_type of 'ctas' or 'insert'")

            found_column = False
            order_by_clause = ''
            for token in self.args.report_order_by:
                if token.upper() in ['ASC', 'DESC']:
                    if not found_column:
                        self.args_parser.error("invalid --report_order_by: %s" % ' '.join(self.args.report_order_by))
                    else:
                        found_column = False
                        order_by_clause += ' ' + token.upper()
                else:
                    if token not in report_columns:
                        self.args_parser.error("order column '%s' is not one of the report columns %s"
                            % (token, pprint.PrettyPrinter().pformat(report_columns)) )
                    if len(order_by_clause) != 0:
                        order_by_clause += ', '
                    order_by_clause += Common.qa(token)
                    found_column = True
            self.args.report_order_by = order_by_clause
    
    def args_process(self):
        """Process arguments.

        Convert argument strings to objects and assign to the class.
        """
        self.args = self.args_parser.parse_args()

        if self.config['report_columns']:
            self.process_report_args()

        # TODO turned off color for Powershell, it seems to partially work in Powershell
        if self.args.nocolor or Common.is_windows:
            Text.nocolor = True

        Common.verbose = self.args.verbose

        return self.args

    @staticmethod
    def DBConnect(description):
        config = Util.config_default.copy()
        config['description'] = description

        args_handler = ArgsHandler(config, init_default=False)
        args_handler.args_process_init()
        args_handler.args_add_optional()
        args_handler.args_add_connection_group()
        args_handler.args = args_handler.args_process()

        if args_handler.args.W:
            args_handler.args.pwd = getpass.getpass("Enter db user password: ")
        else:
            args_handler.args.pwd = None

        return args_handler

class ArgDate:
    """Custom argparse type representing a date
    """
    def __call__(self, arg):
        try:
            value = datetime.strptime(arg, "%Y-%m-%d")
        except ValueError:
            msg='Not a valid date: %s' % arg
            raise argparse.ArgumentTypeError(msg)

        return value

class ArgIntRange:
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

class DBFilterArgs:
    """Class that handles database objects that are used as a filter
    """

    def __init__(self
        , required_args_single
        , optional_args_single
        , optional_args_multi
        , args_handler):
        """During init the command line filter arguments are built for the
        requested object_types

        :param required_args_single: A list of required db object types that will
            be filtered, like: ['db', 'owner', 'table']
        :param optional_args_single: A list of optional db object types that will
            be filtered for a single object, like: ['db', 'owner', 'table']
        :param optional_args_multi: A list of optional db object types that will
            be filtered for multiple objects, like: ['db', 'owner', 'table']
        :param args_handler: the args_handler object created by the caller, this is needed
            to get a handle for args_handler.argparser and args_handler.args
        """
        self.required_args_single = required_args_single
        self.optional_args_single = optional_args_single
        self.optional_args_multi = optional_args_multi
        self.schema_is_required = False
        self.args_handler = args_handler

        if len(self.required_args_single):
            args_filter_grp = (
                self.args_handler.args_parser.add_argument_group(
                    'required database object filter arguments'))
            for otype in self.required_args_single:
                self.args_add_object_type_single(
                    otype, args_filter_grp, True)

        if (len(self.optional_args_single)
            or len(self.optional_args_multi)):
            args_filter_grp = (
                self.args_handler.args_parser.add_argument_group(
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
        filter_grp.add_argument(
            "--%s_in" % otype
            , dest="%s_in_list" % otype
            , nargs="+", action='append', metavar="%s_NAME" % otype.upper(),
            help="%s/s in the list" % otype)
        filter_grp.add_argument(
            "--%s_NOTin" % otype
            , dest="%s_not_in_list" % otype
            , nargs="+", action='append', metavar="%s_NAME" % otype.upper()
            , help="%s/s NOT in the list" % otype)
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
            ret_value = eval('self.args_handler.args.%s' % otype)

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
        arg_in_list = eval('self.args_handler.args.%s_in_list' % otype)
        if arg_in_list:
            arg_in_list = sorted(set(sum(arg_in_list, [])))
        arg_like_pattern = eval('self.args_handler.args.%s_like_pattern' % otype)
        if arg_like_pattern:
            arg_like_pattern = sorted(set(sum(arg_like_pattern, [])))
        arg_not_in_list = eval('self.args_handler.args.%s_not_in_list' % otype)
        if arg_not_in_list:
            arg_not_in_list = sorted(set(sum(arg_not_in_list, [])))
        arg_not_like_pattern = eval(
            'self.args_handler.args.%s_not_like_pattern' % otype)
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
            self.args_handler.args.schema_like_pattern = [['%']]

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
                and self.args_handler.args.schema != None)):
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
        arg_value = eval('self.args_handler.args.%s' % otype)
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
                    objects.append("'%s'" % name)
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
                    objects.append("'%s'" % name)
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

class Text:
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
        return u'\033[%d;%d;%dm' % (
            Text.styles[style.lower()]
            , 30 + Text.colors[fg.lower()]
            , 40 + Text.colors[bg.lower()])

    @staticmethod
    def color(txt, fg='white', bg='black', style='no_effect'):
        """Style a given string with color.

        :param txt: The text input string
        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string with added color
        """
        colored_text = u'%s%s%s' % (
            Text.color_str(fg, bg, style), txt, Text.color_str())

        return txt if Text.nocolor else colored_text

class DBConnect:
    conn_args = {
        'dbuser':'YBUSER'
        , 'host':'YBHOST'
        , 'port':'YBPORT'
        , 'conn_db':'YBDATABASE'}
    env_to_set = conn_args.copy()
    env_to_set['pwd'] = 'YBPASSWORD'

    def __init__(self, args_handler=None, env=None, conn_type=''
        , connect_timeout=10, on_fail_exit=True):
        """Creates a validated database connection object.
        The connection settings can be received as a set of input arguments or
        as environment strings but not both.

        :param args: db setting arguments received from the command line
        :param env: db setting received as environment strings
        :param conn_type: used to name the connection in the case you require
        more than 1 db connection, like; source and destination
        :param connect_timeout: database timeout in seconds when trying to
        connect, defaults to 10 seconds
        :param on_fail_exit: on a failed db connection exit with an error
        , default to True
        """
        self.database = None
        self.schema = None
        self.connect_timeout = connect_timeout
        self.on_fail_exit = on_fail_exit
        self.connected = False
        self.env_pre = self.get_env()

        arg_conn_prefix = ('' if (conn_type=='') else ('%s_' % conn_type))
        pwd_required = False
        self.env = {'pwd':None}
        self.env_set_by = {}
        self.env_args = {}

        if args_handler:
            for conn_arg in self.conn_args.keys():
                conn_arg_qualified = '%s%s' % (arg_conn_prefix, conn_arg)
                #if not hasattr(args, conn_arg_qualified):
                #    sys.stderr.write('Missing Connection Argument: %s\n'
                #        % Text.color(conn_arg_qualified, fg='red'))
                #    exit(2)
                self.env_args[conn_arg] = getattr(args_handler.args, conn_arg_qualified)
                self.env[conn_arg] = self.env_args[conn_arg] or self.env_pre[conn_arg]
                if self.env_args[conn_arg]:
                    self.env_set_by[conn_arg] = 'a'
                elif self.env_pre[conn_arg]:
                    self.env_set_by[conn_arg] = 'e'
                else:
                    self.env_set_by[conn_arg] = 'd'
            pwd_required = getattr(args_handler.args, '%sW' % arg_conn_prefix)
            self.current_schema = getattr(
                args_handler.args, '%scurrent_schema' % arg_conn_prefix)
        elif env:
            self.current_schema = None
            for env_var in env.keys():
                self.env[env_var] = env[env_var] or self.env_pre[env_var]
                if self.env[env_var]:
                    self.env_set_by[env_var] = 'a'
                elif self.env_pre[env_var]:
                    self.env_set_by[env_var] = 'e'
                else:
                    self.env_set_by[env_var] = 'd' 
        else:
            #TODO either args or env should be defined otherwise throw an error
            None

        if not self.env['host']:
            args_handler.args_parser.error("the host database server must "
                "be set using the YBHOST environment variable or with "
                "the argument: --%shost" % arg_conn_prefix)

        if not self.env['pwd']:
            user = (self.env['dbuser']
                or os.environ.get("USER") #Linux
                or os.environ.get("USERNAME") ) #Windows

            if user:
                ybpass_pwd = self.get_ybpass(self.env, user)

                if pwd_required or (self.env_pre['pwd'] is None and ybpass_pwd is None):
                    prompt = ("Enter the password for cluster %s, user %s: "
                        % (Text.color(self.env['host'], fg='cyan')
                            , Text.color(user, fg='cyan')))
                    self.env['pwd'] = getpass.getpass(prompt)
                else:
                    self.env['pwd'] = self.env_pre['pwd'] if self.env_pre['pwd'] else ybpass_pwd
            # if user is missing set an invalid password to simulate a failed login
            else:
                self.env['pwd'] = '-*-force bad password-*-'

        self.verify()

    @staticmethod
    def get_ybpass(user_env, user):
        env = user_env.copy()
        env['port'] = env['port'] if env['port'] else '5432'
        env['dbuser'] = env['dbuser'] if env['dbuser'] else user
        env['conn_db'] = env['conn_db'] if env['conn_db'] else user

        try:
            ybpassfile = os.environ.get("YBPASSFILE")
            if not ybpassfile:
                if Common.is_windows:
                    ybpassfile = os.path.expandvars('%APPDATA%\postgresql\pgpass.conf')
                else:
                    ybpassfile = '%s/.ybpass' % os.path.expanduser('~')

            test_ybpassfile = os.access(ybpassfile, os.R_OK)

            if not Common.is_windows:
                # Linux check if file does not have group and world access
                test_ybpassfile = oct(os.stat(ybpassfile).st_mode)[-2:] == '00'
        except:
            test_ybpassfile = False

        pwd = None
        if test_ybpassfile:
            ybpass_data = Common.read_file(ybpassfile, on_read_error_exit=False)
            # regex parses ybpass file, it ignores lines that start with '#' and handles the '\:' and '\\' escape strings 
            regex = r"^(?!#)(((\\:)|(\\\\)|[\x21-\x39]|[\x3b-\x5b]|[\x5d-\x7e])*):(\d*|\*):(((\\:)|(\\\\)|[\x21-\x39]|[\x3b-\x5b]|[\x5d-\x7e])*):(((\\:)|(\\\\)|[\x21-\x39]|[\x3b-\x5b]|[\x5d-\x7e])*):(((\\:)|(\\\\)|[\x20-\x39]|[\x3b-\x5b]|[\x5d-\x7e])*)$"
            matches = re.finditer(regex, ybpass_data, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                env_ybpass = DBConnect.create_env(host=match.group(1), port=match.group(5)
                    , conn_db=match.group(6), dbuser=match.group(10), pwd=match.group(14))
                pwd = DBConnect.get_ybpass_on_env_match(env, env_ybpass)
                if pwd:
                    break
        return pwd

    @staticmethod
    def get_ybpass_on_env_match(env, env_ybpass):
        for cred in ['dbuser', 'host', 'port', 'conn_db']:
            cred_val = env_ybpass[cred].replace('\\\\', '\\').replace('\\:', ':')
            if not(cred_val == '*' or cred_val == env[cred]):
                return None                
        return env_ybpass['pwd'].replace('\\\\', '\\').replace('\\:', ':')

    @staticmethod
    def set_env(env):
        for key, value in env.items():
            env_name = DBConnect.env_to_set[key]
            if value:
                os.environ[env_name] = value
            elif env_name in os.environ:
                del os.environ[env_name]

    @staticmethod
    def get_env():
        env = {}
        for key in DBConnect.env_to_set.keys():
            env_name = DBConnect.env_to_set[key]
            env[key] = os.environ.get(env_name)
        return env

    @staticmethod
    def create_env(dbuser=None, host=None, port=None, conn_db=None, pwd=None):
        return {
            'dbuser':dbuser
            , 'host':host
            , 'port':port
            , 'conn_db':conn_db
            , 'pwd':pwd}

    def verify(self):
        cmd_results = self.ybsql_query(
            """SELECT
    CURRENT_DATABASE() AS db
    , CURRENT_SCHEMA AS schema
    , (SELECT encoding FROM sys.database WHERE name = CURRENT_DATABASE()) AS server_encoding
    , SPLIT_PART(VERSION(), ' ', 4) AS version
    , SPLIT_PART(version, '-', 1) AS version_number
    , SPLIT_PART(version, '-', 2) AS version_release
    , SPLIT_PART(version_number, '.', 1) AS version_major
    , SPLIT_PART(version_number, '.', 2) AS version_minor
    , SPLIT_PART(version_number, '.', 3) AS version_patch
    , rolsuper                       AS is_super_user
    , rolcreaterole OR is_super_user AS has_create_user
    , rolcreatedb OR is_super_user   AS has_create_db
    , CURRENT_USER                   AS user
FROM pg_catalog.pg_roles
WHERE rolname = CURRENT_USER""")

        db_info = cmd_results.stdout.split('|')
        if cmd_results.stderr == '' and cmd_results.exit_code == 0:
            self.database = db_info[0]
            self.schema = db_info[1]
            # if --current_schema arg was set check if it is valid
            # the sql CURRENT_SCHEMA will return an empty string
            if len(self.schema) == 0:
                Common.error('schema "%s" does not exist'
                    % self.current_schema)
            self.connected = True
        else:
            if self.on_fail_exit:
                Common.error(cmd_results.stderr.replace('util', 'ybsql')
                        , cmd_results.exit_code)
            else:
                self.connect_cmd_results = cmd_results
                return

        self.ybdb = {
            'version': db_info[3]
            , 'version_number': db_info[4]
            , 'version_release': db_info[5]
            , 'version_major': int(db_info[6])
            , 'version_minor': int(db_info[7])
            , 'version_patch': int(db_info[8])
            , 'version_number_int': (
                int(db_info[6]) * 10000
                + int(db_info[7]) * 100
                + int(db_info[8]))
            , 'is_super_user': (True if db_info[9].strip() == 't' else False)
            , 'has_create_user': (True if db_info[10].strip() == 't' else False)
            , 'has_create_db': (True if db_info[11].strip() == 't' else False)
            , 'user': db_info[12].strip()
            , 'host': self.env['host']
            , 'database_encoding': db_info[2] }

        if Common.verbose >= 1:
            print(
                '%s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s'
                % (
                    Text.color('Connecting to Host', style='bold')
                    , Text.color(self.env['host'], fg='cyan')
                    , Text.color('Port', style='bold')
                    , Text.color(self.env['port'], fg='cyan')
                    , Text.color('DB User', style='bold')
                    , Text.color(self.env['dbuser'], fg='cyan')
                    , Text.color('Super User', style='bold')
                    , Text.color(self.ybdb['is_super_user'], fg='cyan')
                    , Text.color('Database', style='bold')
                    , Text.color(self.database, fg='cyan')
                    , Text.color('Current Schema', style='bold')
                    , Text.color(self.schema, fg='cyan')
                    , Text.color('DB Encoding', style='bold')
                    , Text.color(self.ybdb['database_encoding'], fg='cyan')
                    , Text.color('YBDB', style='bold')
                    , Text.color(self.ybdb['version'], fg='cyan')))
        #TODO fix this block
        """
        if self.Common.args.verbose >= 2:
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
        """

    def exit_if_not_su(self):
        if not self.ybdb['is_super_user']:
            Common.error('this utility must be run by a database super user...')

    ybsql_call_count = 0
    ybtool_stderr_strip_warnings = [
        'WARNING:  setting the restricted parameter "ybd_analyze_after_writes" may lead to unexpected system behavior']
    def ybsql_query(self, sql_statement
        , options = '-A -q -t -v ON_ERROR_STOP=1 -X', stdin = None, strip_warnings=[]):
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
        self.ybsql_call_count += 1
        strip_warnings.extend(self.ybtool_stderr_strip_warnings)

        sql_statement = ("SET ybd_query_tags TO 'YbEasyCli:%s:ybsql(%d)';\n%s"
            % (Common.util_name, self.ybsql_call_count, sql_statement))
        if self.current_schema:
            sql_statement = "SET SCHEMA '%s';\n%s" % (
                self.current_schema, sql_statement)

        # default timeout is 75 seconds changing it to self.connect_timeout
        #   'host=<host>' string is required first to set command line connect_timeout
        #   see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        ybsql_cmd = "ybsql %s 'host=%s connect_timeout=%d'" % (
            options
            , self.env['host']
            , self.connect_timeout)

        if Common.is_windows:
            ybsql_cmd = """$sql = @'
%s
'@; echo $sql | {ybsql_cmd}""".format(ybsql_cmd=ybsql_cmd)
        else:
            ybsql_cmd = """{ybsql_cmd} <<eof
%s
eof""".format(ybsql_cmd=ybsql_cmd)

        ybsql_cmd = ybsql_cmd % sql_statement

        cmd = self.ybtool_cmd(ybsql_cmd, stack_level=4, stdin=stdin, strip_warnings=strip_warnings)

        return cmd

    def ybtool_cmd(self, cmd, stack_level=3, stdin=None, strip_warnings=[]):
        self.set_env(self.env)
        cmd = Cmd(cmd, stack_level=stack_level, stdin=stdin)
        self.set_env(self.env_pre)

        for warning in strip_warnings:
            cmd.stderr = re.sub(warning, '', cmd.stderr, 0, re.MULTILINE | re.DOTALL).lstrip()

        return cmd

class StoredProc:
    def __init__(self, proc_name, db_conn=None):
        self.db_conn = db_conn
        self.proc_parse_file(proc_name)

    @staticmethod
    def proc_file(proc_name):
        return Common.util_dir_path + ('/sql/%s.sql' % proc_name)

    @staticmethod
    def proc_file_exists(proc_name):
        return os.access(StoredProc.proc_file(proc_name), os.R_OK)

    def proc_parse_file(self, proc_name):
        filepath = StoredProc.proc_file(proc_name)
        self.proc_sql = Common.read_file(filepath)

        regex = r"CREATE\s*(OR\s*REPLACE)?\s*PROCEDURE\s*([a-z0-9_.]+)\s*\((.*?)\)\s*(RETURNS(\s*SETOF)?\s*([a-zA-Z_.]*).*?)\s+.+?(DECLARE\s*(.+))RETURN\s*(NEXT|QUERY\s*EXECUTE)?\s*([^;]*);(.*)\$proc\$"
        matches = re.search(regex, self.proc_sql, re.IGNORECASE | re.DOTALL)

        if not matches:
            Common.error("Stored proc '%s' regex parse stored proc failed." % proc_name)

        self.proc_name          = matches.group(2)
        self.proc_args          = matches.group(3)
        #TODO currently return_type only handles 1 word like; BOOLEAN
        self.proc_is_setof      = (matches.group(5) is not None)
        if not self.proc_is_setof:
            self.proc_return_type = matches.group(6).upper()
        else:
            self.proc_return_table_type = matches.group(6)
        self.proc_before_return = matches.group(8)
        self.proc_setof_return  = matches.group(9)
        self.proc_return        = matches.group(10)
        self.proc_after_return  = matches.group(11)

        #strip sql_inject check from anonymous block as it serves no purpose in an
        #  anonymous block and it won't be properly found as it only exists in the sysviews db
        self.proc_before_return = re.sub(r'.*PERFORM\s*sql_inject_check_p.*\n'
            , '', self.proc_before_return, 0, re.IGNORECASE)

        self.proc_args_parse()

        if self.proc_is_setof:
            self.parse_setof_create_table(as_temp_table=True)
            self.proc_before_return = re.sub(
                r"([a-z0-9_.]+\.)?([a-z0-9_]+)%ROWTYPE", ('%s%%ROWTYPE' % self.new_table_name)
                , self.proc_before_return, re.IGNORECASE)
        else:
            if self.proc_return_type not in ('BOOLEAN', 'BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
                Common.error('Unhandled proc return_type: %s' % self.proc_return_type)

    def proc_args_parse(self):
        self.args = []
        for arg_str in Common.split(self.proc_args):
            arg = {}
            matches = re.search(r'(.*)\bDEFAULT\b(.*)'
                , arg_str, re.DOTALL | re.IGNORECASE)
            if matches:
                arg['def'] = matches.group(1).strip()
                arg['default_value'] = matches.group(2).strip()
            else:
                arg['def'] = arg_str.strip()
                arg['default_value'] = None

            matches = re.search(r'([a-zA-Z0-9_.]+)\b\s*([ a-zA-z]+)(.*)'
                , arg['def'], re.DOTALL | re.IGNORECASE)
            arg['name'] = matches.group(1).strip()
            arg['type'] = matches.group(2).strip()
            arg['type_size'] = matches.group(3).strip()

            self.args.append(arg)

    def parse_setof_create_table(self, new_table_name=None, as_temp_table=False):
        # get the CREATE TABLE
        regex = r"((CREATE\s*(OR\s*REPLACE)?\s*TABLE\s*)([a-z0-9_.]+\.)?([a-z0-9_]+)[^;]*;)"
        matches = re.search(regex, self.proc_sql, re.IGNORECASE | re.DOTALL)

        # strip everything off the CREATE TABLE after the column definitions
        regex = (r"((ON\s*COMMIT|SORT\s*ON|CLUSTER\s*ON|PARTITION\*BY|DISTRIBUTE\s*ON|DISTRIBUTE\s*REPLICATE|DISTRIBUTE\s*RANDOM)[^;]*)")
        result = re.sub(regex, '', matches.group(1), 1, re.IGNORECASE | re.DOTALL)

        # parse CREATE TBALE statement
        regex = r"(CREATE\s*(OR\s*REPLACE)?\s*TABLE\s*)([a-z0-9_.]+\.)?([a-z0-9_]+)\s*\((\s*[^;]*)\)"
        matches = re.search(regex, result, re.IGNORECASE | re.DOTALL)

        if not matches:
            Common.error("Stored proc '%s' regex parse table failed." % self.proc_name)
        self.new_table_name = new_table_name if new_table_name else ('%s_%s' % (matches.group(4), Common.get_uid()))
        temp_clause = ' TEMP' if as_temp_table else ''

        self.create_new_table_sql = ('CREATE%s TABLE %s (%s)'
            % (temp_clause, self.new_table_name, matches.group(5)) )
        
        self.row_cols = []
        self.row_cols_def = {}
        col_defs = re.sub(r"^\s*--.*$", '', matches.group(5), 0, re.MULTILINE) # strip commented columns

        for col_def_str in Common.split(col_defs):
            col_def = {}
            matches = re.search(r'^\s*"?([^\s"]*)\"?(.*)$'
                , col_def_str, re.DOTALL | re.IGNORECASE)
            col_name = matches.group(1).strip()
            col_def['type'] = matches.group(2).strip()
            col_def['def'] = col_def_str

            self.row_cols.append(col_name)
            self.row_cols_def[col_name] = col_def

    def get_proc_declaration(self):
        types = []
        for arg in self.args:
            types.append(arg['type'])

        return '%s(%s)' % (self.proc_name, ', '.join(types))

    def input_args_to_args_clause(self, input_args, is_declare=True):
        args_clause = '--arguments\n' if is_declare else '\n'
        delim = ';' if is_declare else ','

        for arg in self.args:
            #print('arg: %s, dt: %s, dts: %s, default: %s' % (arg, arg_datatype, arg_datatype_size, default))
            if arg['name'] in input_args:
                if arg['type'] == 'VARCHAR':
                    arg_type = ' %s%s' % (arg['type'], arg['type_size'])
                    arg_value = '$a$%s$a$' % input_args[arg['name']]
                elif arg['type'] in ('BOOLEAN', 'BIGINT', 'DATE', 'INT', 'INTEGER', 'NUMERIC', 'SMALLINT'):
                    arg_type = ' %s' % arg['type']
                    arg_value = input_args[arg['name']]
                elif arg['type'] == 'TIMESTAMP':
                    arg_type = ' %s' % arg['type']
                    #arg_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.US')" % input_args[arg['name']].strftime('%Y-%m-%d %H:%M:%S.%f')
                    arg_value = "'%s'::TIMESTAMP" % input_args[arg['name']].strftime('%Y-%m-%d %H:%M:%S.%f')
                else:
                    Common.error('Unhandled proc arg_type: %s' % arg['type'])
            elif arg['default_value']:
                arg_type = ' %s' % arg['type']
                arg_value = arg['default_value']
            else:
                Common.error("Missing proc arg: %s for proc: %s"
                    % (arg['name'], self.proc_name))
            if not is_declare:
                arg_type = ''
            args_clause += ("    %s%s := %s%s\n" % (arg['name'], arg_type, arg_value, delim))

        if not is_declare:
            args_clause = args_clause[:-2]
        return args_clause

    def process_anonymous_block_result(self, cmd_result):
        # pg/plsql RAISE INFO commands are sent to stderr.  The following moves
        #   the RAISE INFO data to be returned as stdout.
        if cmd_result.stderr.strip() != '':
            return_value = None
            # TODO need to figure out howto split the real stderr from stderr RAISE INFO output
            stderr = ''
            stdout = cmd_result.stdout
            stdout_lines = []
            for line in cmd_result.stderr.split('\n'):
                if line[0:20] == 'INFO:  >!>RETURN<!<:':
                    return_value = line[20:].strip()
                elif line[0:7] == 'INFO:  ':
                    stdout_lines.append(line[7:])
                else:
                    stdout_lines.append(line)
            stdout += '\n'.join(stdout_lines)

            if not return_value:
                Common.error(cmd_result.stderr)

            cmd_result.stderr = stderr
            cmd_result.stdout = stdout

            if self.proc_return_type == 'BOOLEAN':
                boolean_values = {'t': True, 'f': False, '<NULL>': None}
                cmd_result.proc_return = boolean_values.get(
                    return_value, None)
            elif self.proc_return_type in ('BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
                cmd_result.proc_return = (
                    None if return_value == '<NULL>' else int(return_value) )
            else:
                Common.error("Unhandled proc return_type: %s" % self.proc_return_type)

        return cmd_result

    def call_proc_as_anonymous_block(self
        , args={}
        , pre_sql=''
        , post_sql=''):
        """Convert an SQL stored procedure to an anonymous SQL block,
        then execute the anonymous SQL block.  This allows a user to run
        the stored procedure without building the procedure, lowering the
        barrier to run.

        :param args: a dictionary of input args/values to use when calling the stored proc
        :param pre_sql: SQL to execute before the stored proc
        :param post_sql: SQL to execute after the stored proc
        """
        return_marker = '>!>RETURN<!<:'

        declare_clause_args = self.input_args_to_args_clause(args)

        anonymous_block = """
{pre_sql}
--proc: {proc_name}
DO $PROC$
DECLARE
    {declare_clause_args}
    --variables
    {proc_before_return}
    RAISE INFO '{return_marker}%', {proc_return};
    {proc_after_return} $PROC$;
{post_sql}""".format(
            pre_sql=pre_sql, post_sql=post_sql
            , proc_name=self.proc_name
            , declare_clause_args=declare_clause_args
            , proc_before_return=self.proc_before_return
            , return_marker=return_marker, proc_return=self.proc_return
            , proc_after_return=self.proc_after_return )

        cmd_result = self.db_conn.ybsql_query(anonymous_block)
        return self.process_anonymous_block_result(cmd_result)

    def proc_setof_to_anonymous_block(self
        , args={}
        , pre_sql=''
        , post_sql=''):
        """Convert an SQL stored procedure which returns a setof to an
        anonymous SQL block.  This allows a user to run
        the stored procedure without building the procedure, lowering the
        barrier to run.

        :param args: a dictionary of input args/values to use when calling the stored proc
        :param pre_sql: SQL to execute before the stored proc
        :param post_sql: SQL to execute after the stored proc
        """

        declare_clause_args = self.input_args_to_args_clause(args)

        var_ret_rec=''
        if self.proc_setof_return == 'NEXT':
            insert_stmt = 'INSERT INTO {new_table_name} VALUES ({proc_return}.*)'
        elif self.proc_setof_return == 'QUERY EXECUTE':
            var_ret_rec = """_{new_table_name}_rec {new_table_name}%ROWTYPE;
    """.format(new_table_name=self.new_table_name)
            insert_stmt = """FOR _{new_table_name}_rec IN EXECUTE( {proc_return} ) 
    LOOP
        INSERT INTO {new_table_name} VALUES (_{new_table_name}_rec.*);
    END LOOP"""
        else:
            Common.error("Stored proc '%s' unhandled setof return clause '%s'." % (
                self.proc_name, self.proc_setof_return) )

        insert_stmt = insert_stmt.format(
                new_table_name=self.new_table_name, proc_return=self.proc_return )

        anonymous_block = """
{pre_sql}
{create_tmp_table};
--proc: {proc_name}
DO $PROC$
DECLARE
    {declare_clause_args}
    --variables
    {var_ret_rec}{proc_before_return}
    {insert_stmt};
    {proc_after_return} $PROC$;
{post_sql}""".format(
            pre_sql=pre_sql, post_sql=post_sql
            , create_tmp_table=self.create_new_table_sql
            , proc_name=self.proc_name
            , declare_clause_args=declare_clause_args
            , var_ret_rec=var_ret_rec
            , proc_before_return=self.proc_before_return
            , insert_stmt=insert_stmt
            , proc_after_return=self.proc_after_return )

        return(self.new_table_name, anonymous_block)

class Report:
    def __init__(self, args_handler, db_conn, columns, query, pre_sql='', strip_warnings=[]):
        self.args_handler = args_handler
        self.db_conn = db_conn
        self.columns = columns
        self.query = query
        self.pre_sql = pre_sql
        self.strip_warnings = strip_warnings

    @staticmethod
    def del_data_to_list_data(del_data, delimiter='|'):
        raw_data = del_data.split('\n', 1) # split the first row the header row from the data rows
        headers = raw_data[0].split(delimiter)

        regex = r"(^([^{delimiter}]*{delimiter}){{{num_headers_minus_1}}}[^{delimiter}]*$)".format(
            num_headers_minus_1=(len(headers)-1)
            , delimiter=('\\%s' % hex(ord(delimiter))[1:]) )

        data = []
        matches = re.finditer(regex, raw_data[1], re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            data.append(match.groups(0)[0].split(delimiter))

        return (headers, data)

    def list_data_sort(self, headers, list_data):
        if (hasattr(self.args_handler.args, 'report_sort_column')
            and self.args_handler.args.report_sort_column in headers):
            sort_index = headers.index(self.args_handler.args.report_sort_column)
            list_data.sort(
                key=lambda x: x[sort_index]
                , reverse=self.args_handler.args.report_sort_reverse)
        return (headers, list_data)

    #def list_data_filtered(self, headers, list_data):
    #    #the include list will also reorder the columns as supplied
    #    if (hasattr(self.args_handler.args, 'report_include_columns')
    #        and self.args_handler.args.repox`rt_include_columns):
    #        new_headers = [header for header in self.args_handler.args.report_include_columns if header in headers]
    #        new_data = []
    #        for i in range(0,len(list_data)):
    #            new_data.append([None]*len(new_headers))
    #        new_col_index = 0
    #        for header in new_headers:
    #            col_index = headers.index(header)
    #            row_index = 0
    #            for row in list_data:
    #                new_data[row_index][new_col_index] = row[col_index]
    #                row_index += 1
    #            new_col_index += 1
    #        list_data = new_data
    #        headers = new_headers

    #    if (hasattr(self.args_handler.args, 'report_exclude_columns')
    #        and self.args_handler.args.report_exclude_columns):
    #        index = len(headers)
    #        for header in reversed(headers):
    #            index -= 1
    #            if header in self.args_handler.args.report_exclude_columns:
    #                for row in list_data:
    #                    del row[index]
    #        headers = [header for header in headers if header not in self.args_handler.args.report_exclude_columns]

    #    return (headers, list_data)

    def del_data_to_formatted_report(self, del_data, delimiter='|'):
        (headers, data) = Report.del_data_to_list_data(del_data, delimiter)
        (headers, data) = self.list_data_sort(headers, data)
        #(headers, data) = self.list_data_filtered(headers, data)

        headers_formatted = [header.replace('_', '\n') for header in headers]
        return tabulate(data, headers=headers_formatted)

    def del_data_processed(self, del_data, delimiter='|'):
        (headers, data) = Report.del_data_to_list_data(del_data, delimiter)
        (headers, data) = self.list_data_sort(headers, data)
        #(headers, data) = self.list_data_filtered(headers, data)

        del_data = [delimiter.join(headers)]
        for row in data:
            del_data.append(delimiter.join(row))
        return '\n'.join(del_data)

    def build(self, is_source_cstore=False):
        args = self.args_handler.args

        query = """WITH
report_data AS (
    {query}
)
SELECT
    {at}{columns}
FROM report_data""".format(
            query=self.query
            , at=('LOCALTIMESTAMP AS "at", ' if args.report_add_ts_column else '')
            , columns=('\n    , '.join(map(Common.qa, self.columns))) )

        #case 1 create printed report
        if args.report_type in ('formatted', 'psv'):
            if args.report_type == 'formatted':
                delimiter = chr(31) # ASCII US(unit separator)
            elif args.report_type == 'psv':
                delimiter = args.report_delimiter

            escape_str = '\\' if Common.is_windows else '\\\\'
            fieldsep_clause = "'%s%s'" % (escape_str, hex(ord(delimiter))[1:])

            query = """
\pset tuples_only off
\pset footer off
\pset fieldsep {fieldsep_clause}
{pre_sql}{query}""".format(
                fieldsep_clause=fieldsep_clause
                , pre_sql=self.pre_sql
                , query=query)

            self.cmd_results = self.db_conn.ybsql_query(query, strip_warnings=self.strip_warnings)
            self.cmd_results.on_error_exit()

            if args.report_type == 'formatted':
                report = self.del_data_to_formatted_report(self.cmd_results.stdout, delimiter)
            elif args.report_type == 'psv':
                report = self.del_data_processed(self.cmd_results.stdout, delimiter)

        elif args.report_type in ('ctas', 'insert'):
            #case 2 store report from cstore table
            if (is_source_cstore):
                if args.report_type == 'ctas':
                    table_sql = 'CREATE TABLE %s AS ' % Common.quote_object_paths(args.report_dst_table)
                else:
                    table_sql = 'INSERT INTO %s ' % Common.quote_object_paths(args.report_dst_table)
                query = """
{pre_sql}{table_sql}{query}{dist_clause}""".format(
                        pre_sql=self.pre_sql
                        , table_sql=table_sql
                        , query=query
                        , dist_clause=(' DISTRIBUTE RANDOM' if args.report_type == 'ctas' else '') )

                self.cmd_results = self.db_conn.ybsql_query(query, strip_warnings=self.strip_warnings)
                self.cmd_results.on_error_exit()

            #case 3 store report from rstore table
            else:
                from yb_sys_query_to_user_table import sys_query_to_user_table

                args_handler = copy.copy(self.args_handler)
                if args.report_type == 'ctas':
                    args_handler.args.create_table = True
                args_handler.args.query = query
                args_handler.args.pre_sql = self.pre_sql
                args_handler.args.table = args.report_dst_table

                sqtout = sys_query_to_user_table(db_conn=self.db_conn, args_handler=args_handler)
                sqtout.execute()
                sqtout.cmd_results.on_error_exit()

            report = '--Report type "%s" completed' % args.report_type

        return report


class Util(object):
    conn_args_file = {'$HOME/conn.args': """--host yb89
--dbuser dze
--conn_db stores"""}

    config = {}
    config_default = {
        'description': None
        , 'required_args_single': []
        , 'optional_args_single': ['schema']
        , 'optional_args_multi': []
        , 'positional_args_usage': None
        , 'default_args': {}
        , 'usage_example': {
            'cmd_line_args': '@$HOME/conn.args'
            , 'file_args': [ conn_args_file ] }
        , 'usage_example_extra': {}
        , 'output_tmplt_vars': None
        , 'output_tmplt_default': None
        , 'db_filter_args': {}
        , 'additional_args': None
        , 'report_sp_location': '.'
        , 'report_columns': None
        , 'report_default_order': [] }

    def __init__(self, db_conn=None, args_handler=None, init_default=True, util_name=None):
        if util_name:
            self.util_name = util_name
        else:
            self.util_name = self.__class__.__name__

        for k, v in Util.config_default.items():
            if k not in self.config.keys():
                self.config[k] = v

        if init_default:
            self.init_default(db_conn, args_handler)

    def init_default(self, db_conn=None, args_handler=None):
        if db_conn: # util called from code with import
            self.db_conn = db_conn
            self.args_handler = args_handler
            for k, v in self.config['default_args'].items():
                if not(hasattr(self.args_handler.args, k)):
                    setattr(self.args_handler.args, k, v)
        else: # util called from the command line
            self.args_handler = ArgsHandler(self.config, init_default=False)
            self.config['additional_args'] = getattr(self, 'additional_args')
            self.args_handler.init_default()
            self.args_handler.args_process()
            self.additional_args_process()
            if Common.verbose >= 3:
                print('args: %s' % pprint.PrettyPrinter().pformat(vars(self.args_handler.args)))
            self.db_conn = DBConnect(self.args_handler)

        if hasattr(self.args_handler, 'db_filter_args'):
            self.db_filter_args = self.args_handler.db_filter_args

    def exec_query_and_apply_template(self, sql_query, exec_output=False):
        self.cmd_result = self.db_conn.ybsql_query(sql_query)
        self.cmd_result.on_error_exit()
        return self.apply_template(self.cmd_result.stdout, exec_output)

    def apply_template(self, output_raw, exec_output=False):
        # convert the SQL from code(of a dictionary) to an evaluated dictionary
        rows = eval('[%s]' % output_raw)

        additional_vars = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            , 'max_ordinal': len(rows)
            , '^M': '\n' }

        output_new = ''
        for row in rows:
            format = {}
            # strip vars and add double quotes to non-lower case db objects
            for k, v in row.items():
                if k in ('column', 'database', 'object', 'owner', 'schema', 'sequence', 'stored_proc', 'table', 'view'):
                    format[k] = Common.quote_object_paths(v.strip())
                elif type(v) is str:
                    format[k] = v.strip()
                else:
                    format[k] = v
            # build *_path vars like table_path and schema_path
            for var in self.config['output_tmplt_vars']:
                path_var = var.rsplit('_',1)
                if len(path_var) == 2 and path_var[1] == 'path':
                    if path_var[0] in ['object', 'sequence', 'stored_proc', 'table', 'view']:
                        format[var] = '%s.%s.%s' % (format['database'], format['schema'], format[path_var[0]])
                    elif path_var[0] in ('schema'):
                        format[var] = '%s.%s' % (format['database'], format['schema'])
                    elif path_var[0] in ('column'):
                        objct = ('table' if ('table' in format) else 'object')
                        format[var] = '%s.%s.%s.%s' % (format['database'], format['schema'], format[objct], format[path_var[0]])

            format.update(additional_vars)
            format.update(self.db_conn.ybdb)
            try:
                output_new += (self.args_handler.args.template.format(**format)
                    + ('\n'))
            except KeyError as error:
                Common.error('%s template var was not found...' % error)

        if exec_output:
            self.cmd_result = self.db_conn.ybsql_query(output_new)
            self.cmd_result.on_error_exit()
            return self.cmd_result.stdout
        else:
            return output_new

    def db_filter_sql(self, db_filter_args='db_filter_args'):
        return self.db_filter_args.build_sql_filter(self.config[db_filter_args])

    def additional_args(self):
        None

    def additional_args_process(self):
        None

    def src_to_dst_table_ddl(self, src_table, dst_table, src_db_conn, dst_db_conn, in_args_handler):
        from yb_ddl_object import ddl_object

        (src_database, src_schema, src_table) = Common.split_db_object_name(src_table)
        args_handler = copy.deepcopy(in_args_handler)
        db_conn = copy.deepcopy(src_db_conn)

        if src_database:
            db_conn.database = src_database
            db_conn.env['conn_db'] = src_database

        ddlo = ddl_object(db_conn=db_conn, args_handler=args_handler)

        ddlo.init_config('table')
        args_handler.args.schema = src_schema if src_schema else db_conn.schema
        args_handler.args.table = src_table
        args_handler.args.template = '{ddl}'
        args_handler.args.with_schema = False
        args_handler.args.with_db = False
        args_handler.args.exec_output = False
        args_handler.db_filter_args = DBFilterArgs(['schema', 'table'], [], [], args_handler)
        ddl = ddlo.execute()

        ddl = re.sub(r'^CREATE TABLE[^(]*', 'CREATE TABLE %s ' % Common.quote_object_paths(dst_table), ddl)
        cmd = dst_db_conn.ybsql_query(ddl)
        cmd.on_error_exit()

    def get_dbs(self, filter_clause=None):
        filter_clause = self.db_filter_args.build_sql_filter({'database':'db_name'})

        sql_query = """
SELECT
    name AS db_name
FROM
    sys.database
WHERE
    {filter_clause}
ORDER BY
    name""".format(filter_clause = filter_clause)

        cmd_result = self.db_conn.ybsql_query(sql_query)
        cmd_result.on_error_exit()

        dbs = cmd_result.stdout.strip()
        if dbs == '' and self.db_filter_args.has_optional_args_multi_set('database'):
            dbs = []
        elif dbs == '':
            dbs = ['"' + self.db_conn.database + '"']
        else:
            dbs = dbs.split('\n')

        return dbs

    def get_cluster_info(self, return_format='dict'):
        sql_query = """
\pset tuples_only off
\pset footer off
WITH
wrkr AS (
    SELECT
        worker_id
        , COUNT(*)         AS drives
        , SUM(total_bytes) AS wrkr_bytes
        , MAX(chassis_id)  AS chassis_id
        , MAX(drive) + 1   AS drives_per_wrkr
        , MIN(total_bytes) AS bytes_drive_min
        , MAX(total_bytes) AS bytes_drive_max
    FROM
        sys.drive_summary
    WHERE drive IS NOT NULL AND total_bytes IS NOT NULL
    GROUP BY
        worker_id
)
, chassis AS (
    SELECT
        COUNT(*)              AS chassis
        , MIN(chassis_wrkrs) AS min_chassis_wrkrs
        , MAX(chassis_wrkrs) AS max_chassis_wrkrs
    FROM (SELECT chassis_id, COUNT(*) AS chassis_wrkrs FROM wrkr GROUP BY chassis_id) as wrkrs
)
, clstr AS (
    SELECT
        MAX(chassis)                  AS chassis
        , MIN(min_chassis_wrkrs)      AS min_chassis_wrkrs
        , MAX(max_chassis_wrkrs)      AS max_chassis_wrkrs
        , COUNT(*)                    AS total_wrkrs
        , MAX(drives_per_wrkr)        AS drives_per_wrkr
        , MIN(bytes_drive_min)        AS bytes_drive_min
        , MAX(bytes_drive_max)        AS bytes_drive_max
        , MIN(bytes_drive_min) * MAX(drives_per_wrkr) AS bytes_wrkr_min
        , MAX(bytes_drive_max) * MAX(drives_per_wrkr) AS bytes_wrkr_max
        , ROUND((1.0 - ((MAX(max_chassis_wrkrs) - 2) / MAX(max_chassis_wrkrs)::NUMERIC)) * 100, 5) AS disk_parity_pct
        , ROUND(bytes_wrkr_max * (disk_parity_pct/100.0)) AS bytes_wrkr_parity
        , MAX(scratch_bytes)          AS bytes_wrkr_temp
        , ROUND(bytes_wrkr_temp / (bytes_wrkr_max * 1.0)*100.0, 5) AS chassis_temp_pct
        , bytes_wrkr_min - bytes_wrkr_parity - bytes_wrkr_temp AS bytes_wrkr_data
    FROM
        wrkr
        LEFT JOIN sys.storage USING (worker_id)
        CROSS JOIN chassis
)
SELECT * FROM clstr
"""
        cmd_result = self.db_conn.ybsql_query(sql_query)
        cmd_result.on_error_exit()
        (headers, data) = Report.del_data_to_list_data(cmd_result.stdout.strip())

        if return_format == 'sql':
            cluster_info = '    SELECT'
        else:
            cluster_info = {}
        for index in range(0,len(headers)):
            if return_format == 'sql':
                cluster_info += '\n        %s%s AS %s' % (
                    '' if index == 0 else ', '
                    , data[0][index]
                    , headers[index])
            else:
                cluster_info[headers[index]] = data[0][index]

        return cluster_info

    def schema_with_db_sql(self):
        """This method creates a schema info query that returns the same columns regardless YB version
        """
        if self.db_conn.ybdb['version_major'] == 3:
            databases = self.get_dbs()
            schema_sql = "SELECT NULL::VARCHAR AS database, NULL::BIGINT AS schema_id, NULL::VARCHAR AS name"
            for database in databases:
                schema_sql += "\n    UNION ALL SELECT '%s', schema_id, name FROM %s.sys.schema" % (
                    database, database)
        else:
            schema_sql = """SELECT d.name AS database, schema_id, s.name
    FROM sys.database AS d JOIN sys.schema AS s USING (database_id)"""

        return schema_sql

    @staticmethod
    def ybsql_py_key_values_to_py_dict(ybsql_py_key_values):
        return """
\\echo {
%s
\\echo }
""" % '\n\\echo ,\n'.join(ybsql_py_key_values)

    @staticmethod
    def sql_to_ybsql_py_key_value(key, sql):
        if key in ('rowcount', 'ordinal'):
            return """\\echo "%s":
%s\n""" % (key, sql)
        else:
            return """\\echo "%s": '""\"'
%s
\\echo '""\"'\n""" % (key, sql)

    @staticmethod
    def dict_to_ybsql_py_key_values(dct):
        ybsql_py_key_values = []
        for k, v in dct.items():
            if k in ('rowcount', 'ordinal'):
                ybsql_py_key_values.append(
                    """\\echo "%s": %s\n""" % (k,v) )
            else:
                ybsql_py_key_values.append(
                    """\\echo "%s": ""\" %s ""\"\n""" % (k,v) )
        return ybsql_py_key_values


class UtilArgParser(argparse.ArgumentParser):
    @staticmethod
    def error(message):
        Common.error('error: %s' % message, no_exit=True)
        #disabling printing of complete help after error as the error scrolls off the screen
        #self.print_help()
        sys.stderr.write("for complete help, execute: %s\n" % (
            Text.color('%s --help' % Common.util_file_name, style='bold') ) )
        sys.exit(1)

def convert_arg_line_to_args(line):
    """This function overrides the convert_arg_line_to_args from argparse.
    It enhances @arg files to have;
        - # comment lines
        - multiline arguments using python style triple double quote notation(in_hard_quote)
    """
    if len(line) and line[0] == '#': # comment line skip
        None
    else:
        if convert_arg_line_to_args.in_hard_quote:
            convert_arg_line_to_args.dollar_str += '\n'
        line_len = len(line)
        loc = 0
        args_str = ''
        while loc < line_len:
            # find '$$' in str
            if line[loc:loc+3] == '"""':
                loc += 3
                convert_arg_line_to_args.in_hard_quote = not convert_arg_line_to_args.in_hard_quote
                if convert_arg_line_to_args.in_hard_quote:
                    if len(args_str) > 0:
                        convert_arg_line_to_args.args.extend(shlex.split(args_str))
                        args_str = ''
                else:
                    if len(convert_arg_line_to_args.dollar_str) > 0:
                        convert_arg_line_to_args.args.append(convert_arg_line_to_args.dollar_str)
            else:
                if convert_arg_line_to_args.in_hard_quote:
                    convert_arg_line_to_args.dollar_str += line[loc]
                else:
                    args_str += line[loc]
                loc += 1
                if loc == line_len and len(args_str) > 0:
                    convert_arg_line_to_args.args.extend(shlex.split(args_str))
                    args_str = ''

    while convert_arg_line_to_args.arg_ct < len(convert_arg_line_to_args.args):
        convert_arg_line_to_args.arg_ct += 1
        yield convert_arg_line_to_args.args[convert_arg_line_to_args.arg_ct-1]

convert_arg_line_to_args.in_hard_quote = False
convert_arg_line_to_args.dollar_str = ''
convert_arg_line_to_args.args = []
convert_arg_line_to_args.arg_ct = 0

# Standalone tests
# Example: yb_common.py -h YB14 -U denav -D denav
if __name__ == "__main__":
    class test_util(Util):
        config = {
            'description': 'User Usage Report.'
            , 'usage_example': {
                'cmd_line_args': "--host yb89 --DB stores --user dze"} }

    test_util = test_util()

    print('Common.version: %s' % Common.version)
    print('Common.start_ts: %s' % Common.start_ts)
    print('Common.util_dir_path: %s' % Common.util_dir_path)
    print('Common.util_file_name: %s' % Common.util_file_name)
    print('Common.util_name: %s' % Common.util_name)
    print('Common.is_windows: %s' % Common.is_windows)
    print('Common.is_cygwin: %s' % Common.is_cygwin)

    print('test_util.args_handler.args: %s' % pprint.PrettyPrinter().pformat(vars(test_util.args_handler.args)))

    print('test_util.db_conn.database: %s' % test_util.db_conn.database)
    print('test_util.db_conn.schema: %s' % test_util.db_conn.schema)
    print('test_util.db_conn.ybdb: %s' % pprint.PrettyPrinter().pformat(test_util.db_conn.ybdb))
    print('test_util.db_conn.env: %s' % re.sub("'pwd':\s*'.*", "'pwd': <Masked>", pprint.PrettyPrinter().pformat(test_util.db_conn.env)))

    # Print extended information on the environment running this program
    print('platform.platform(): %s' % platform.platform())
    print('platform.python_implementation(): %s' % platform.python_implementation())
    print('sys.version: %s' % sys.version)
