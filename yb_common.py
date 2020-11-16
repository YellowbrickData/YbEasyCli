#!/usr/bin/env python3
"""Performs functions such as argument parsing, login verification, logging,
and command execution that are common to all utilities in this package.
"""

from yb_example_usage import example_usage

import argparse
import getpass
import os
import platform
import re
import subprocess
import sys
import traceback
import shlex
import signal
from datetime import datetime

def signal_handler(signal, frame):
    common.error('user terminated...')

signal.signal(signal.SIGINT, signal_handler)


class common:
    version = '20201115'
    verbose = 0

    util_dir_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    util_file_name = os.path.basename(os.path.realpath(sys.argv[0]))
    util_name = util_file_name.split('.')[0]

    def __init__(self):
        """Create an instance of the common library used by all utilities
        """
        self.start_ts = datetime.now()

    @staticmethod
    def error(msg, exit_code=1, color='red', no_exit=False):
        sys.stderr.write("%s: %s\n" % (
            text.color(common.util_file_name, style='bold')
            , text.color(msg, color)))
        if not no_exit:
            exit(exit_code)

    @staticmethod
    def read_file(file_path, on_read_error_exit=True, color='red'):
        data = None
        try:
            with open(file_path) as f:
                data = f.read()
                f.close()
        except IOError as ioe:
            if on_read_error_exit:
                common.error(ioe)

        return data

    @staticmethod
    def call_cmd(cmd_str, stack_level=2):
        """Spawn a new process to execute the given command.

        Example: results = call_cmd('env | grep -i path')

        :param cmd_str: The string representing the command to execute
        :param stack_level: A number signifying the limit of stack trace
                            entries (Default value = 2)
        :return: The result produced by running the given command
        """
        if common.verbose >= 2:
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
        elif common.verbose >= 1:
            print('%s: %s'
                % (text.color('Executing', style='bold'), cmd_str))

        start_time = datetime.now()
        p = subprocess.Popen(
            cmd_str
            , stdout=subprocess.PIPE
            , stderr=subprocess.PIPE
            , shell=True)
        #(stdout, stderr) = map(bytes.decode, p.communicate())

        (stdout, stderr) = p.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")

        end_time = datetime.now()

        results = cmd_results(p.returncode, stdout, stderr)

        if common.verbose >= 2:
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

    @staticmethod
    def ts(self):
        """Get the current time (for time stamping)"""
        return str(datetime.now())

    @staticmethod
    def quote_object_paths(object_paths):
        """Convert database object names to have double quotes where required"""
        quote_object_paths = []
        for object_path in object_paths.split('\n'):
            #first remove all double quotes to start with an unquoted object path
            #   sometimes the incoming path is partially quoted
            object_path = object_path.replace('"', '')
            objects = []
            for objct in object_path.split('.'):
                if len(re.sub('[a-z0-9_]', '', objct)) == 0:
                    objects.append(objct)
                else:
                    objects.append('"' + objct + '"')
            quote_object_paths.append('.'.join(objects))

        return '\n'.join(quote_object_paths)

    @staticmethod
    def split(str, delim=','):
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
                    common.error('Invalid Argument List: %s' % str)
                else:
                    open_char.pop()
            else:
                skip_close = False

        if len(open_char) > 0:
            common.error('Invalid Argument List: %s' % str)
        else:
            tokens.append(token.strip())

        return tokens

    @staticmethod
    def apply_template(input, template, vars):
        output = ''
        vars.append('raw')
        if input:
            for line in input.strip().split('\n'):
                if line[0:2] == '--':
                    out_line = line
                else:
                    out_line = template
                    for var in vars:
                        if var in ('table_path', 'view_path', 'sequence_path'):
                            value = common.quote_object_paths('.'.join(line.split('.')[0:3]))
                        elif var == 'schema_path':
                            value = common.quote_object_paths('.'.join(line.split('.')[0:2]))
                        elif var == 'data_type':
                            value = line.split('.')[5]
                        elif var == 'ordinal':
                            value = line.split('.')[4]
                        elif var == 'column':
                            value = line.split('.')[3]
                        elif var in ('table', 'view', 'sequence'):
                            value = line.split('.')[2]
                        elif var == 'schema':
                            value = line.split('.')[1]
                        elif var == 'database':
                            value = line.split('.')[0]
                        elif var == 'raw':
                            value = line
                        out_line = out_line.replace('<%s>' % var, value)
                output += out_line + '\n'
        return output


class args_handler:
    """This class contains functions used for argument parsing
    """
    def __init__(self
        , description=None
        , required_args_single=[]
        , optional_args_single=['schema']
        , optional_args_multi=[]
        , positional_args_usage='[database]'):
        if description is not None:
            self.init_default(
                description, required_args_single, optional_args_single
                , optional_args_multi, positional_args_usage)

    def init_default(self
        , description, required_args_single
        , optional_args_single, optional_args_multi
        , positional_args_usage):
        """Build all the requested database arguments

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
        
        self.db_filter_args = db_filter_args(
            required_args_single
            , optional_args_single
            , optional_args_multi
            , self)

    def formatter(self, prog):
        return argparse.RawDescriptionHelpFormatter(prog, width=100)

    def args_usage_example(self):
        if common.util_name in example_usage.examples.keys():
            usage = example_usage.examples[common.util_name]
            text = ('example usage:'
                + '\n  ./%s %s' % (common.util_file_name, usage['cmd_line_args']))
            
            if 'file_args' in usage.keys():
                for file_dict in usage['file_args']:
                    for file in file_dict.keys():
                        text = text + "\n\n  file '%s' contains:" % file
                        for line in file_dict[file].split('\n'):
                            text =  text + '\n    ' + line
        else:
            text = None
        return(text)

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

        usage_example = self.args_usage_example()
        if usage_example:
            epilog = '%s%s' % ((epilog if epilog else ''), usage_example)
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
            , action="version", version=common.version
            , help="display the program version and exit")

    def args_process(self):
        """Process arguments.

        Convert argument strings to objects and assign to the class.
        """
        self.args = self.args_parser.parse_args()

        if self.args.nocolor:
            text.nocolor = True

        common.verbose = self.args.verbose

        return self.args


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

    def __init__(self, exit_code, stdout, stderr):
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

    def write(self, head='', tail='', quote=False):
        sys.stdout.write(head)
        if self.stdout != '':
            sys.stdout.write(
                common.quote_object_paths(self.stdout)
                if quote
                else self.stdout)
        if self.stderr != '':
            common.error(self.stderr, no_exit=True)
        else:
            sys.stdout.write(tail)

    def on_error_exit(self, write=True):
        if self.stderr != '' or self.exit_code != 0:
            if write:
                self.write()
            exit(self.exit_code)

class db_filter_args:
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

class db_connect:
    conn_args = {
        'dbuser':'YBUSER'
        , 'host':'YBHOST'
        , 'port':'YBPORT'
        , 'conn_db':'YBDATABASE'}
    env_to_set = conn_args.copy()
    env_to_set['pwd'] = 'YBPASSWORD'

    def __init__(self, args=None, env=None, conn_type=''
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
  
        if args:
            for conn_arg in self.conn_args.keys():
                conn_arg_qualified = '%s%s' % (arg_conn_prefix, conn_arg)
                #if not hasattr(args, conn_arg_qualified):
                #    sys.stderr.write('Missing Connection Argument: %s\n'
                #        % text.color(conn_arg_qualified, fg='red'))
                #    exit(2)
                self.env_args[conn_arg] = getattr(args, conn_arg_qualified)
                self.env[conn_arg] = self.env_args[conn_arg] or self.env_pre[conn_arg]
                if self.env_args[conn_arg]:
                    self.env_set_by[conn_arg] = 'a'
                elif self.env_pre[conn_arg]:
                    self.env_set_by[conn_arg] = 'e'
                else:
                    self.env_set_by[conn_arg] = 'd'
            pwd_required = getattr(args, '%sW' % arg_conn_prefix)
            self.current_schema = getattr(
                args, '%scurrent_schema' % arg_conn_prefix)
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
            common.error("the host database server must "
                "be set using the YBHOST environment variable or with "
                "the argument: --%shost" % arg_conn_prefix)

        if not self.env['pwd']:
            user = self.env['dbuser'] or os.environ.get("USER")
            if user:
                if pwd_required or self.env_pre['pwd'] is None:
                    prompt = ("Enter the password for cluster %s, user %s: "
                        % (text.color(self.env['host'], fg='cyan')
                            , text.color(user, fg='cyan')))
                    self.env['pwd'] = getpass.getpass(prompt)
                else:
                    self.env['pwd'] = self.env_pre['pwd']
            # if user is missing
            # set an invalid password to simulate a failed login
            else:
                self.env['pwd'] = '-*-force bad password-*-'

        self.verify()

    @staticmethod
    def set_env(env):
        for key, value in env.items():
            env_name = db_connect.env_to_set[key]
            if value:
                os.environ[env_name] = value
            elif env_name in os.environ:
                del os.environ[env_name]

    @staticmethod
    def get_env():
        env = {}
        for key, value in db_connect.env_to_set.items():
            env_name = db_connect.env_to_set[key]
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
    , rolsuper AS is_super_user
    , rolcreaterole AS has_create_user
    , rolcreatedb AS has_create_db
FROM pg_catalog.pg_roles
WHERE rolname = CURRENT_USER""")

        db_info = cmd_results.stdout.split('|')
        if cmd_results.exit_code == 0:
            self.database = db_info[0]
            self.schema = db_info[1]
            self.database_encoding = db_info[2]
            # if --current_schema arg was set check if it is valid
            # the sql CURRENT_SCHEMA will return an empty string
            if len(self.schema) == 0:
                common.error('schema "%s" does not exist'
                    % self.current_schema)
            self.connected = True
        else:
            if self.on_fail_exit:
                common.error(cmd_results.stderr.replace('util', 'ybsql')
                        , cmd_results.exit_code)
            else:
                self.connect_cmd_results = cmd_results
                return

        self.ybdb_version = db_info[3]
        self.ybdb_version_number = db_info[4]
        self.ybdb_version_release = db_info[5]
        self.ybdb_version_major = int(db_info[6])
        self.ybdb_version_minor = int(db_info[7])
        self.ybdb_version_patch = int(db_info[8])
        self.ybdb_version_number_int = (
            self.ybdb_version_major * 10000
            + self.ybdb_version_minor * 100
            + self.ybdb_version_patch)
        self.is_super_user = (True if db_info[9].strip() == 't' else False)
        self.has_create_user = (True if db_info[10].strip() == 't' else False)
        self.has_create_db = (True if db_info[11].strip() == 't' else False)

        if common.verbose >= 1:
            print(
                '%s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, %s: %s'
                % (
                    text.color('Connecting to Host', style='bold')
                    , text.color(self.env['host'], fg='cyan')
                    , text.color('Port', style='bold')
                    , text.color(self.env['port'], fg='cyan')
                    , text.color('DB User', style='bold')
                    , text.color(self.env['dbuser'], fg='cyan')
                    , text.color('Super User', style='bold')
                    , text.color(self.is_super_user, fg='cyan')
                    , text.color('Database', style='bold')
                    , text.color(self.database, fg='cyan')
                    , text.color('Current Schema', style='bold')
                    , text.color(self.schema, fg='cyan')
                    , text.color('DB Encoding', style='bold')
                    , text.color(self.database_encoding, fg='cyan')
                    , text.color('YBDB', style='bold')
                    , text.color(self.ybdb_version, fg='cyan')))
        #TODO fix this block
        """
        if self.common.args.verbose >= 2:
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

    def ybsql_query(self, sql_statement
        , options = '-A -q -t -v ON_ERROR_STOP=1 -X'):
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
        if self.current_schema:
            sql_statement = "SET SCHEMA '%s';\n%s" % (
                self.current_schema, sql_statement)

        # default timeout is 75 seconds changing it to self.connect_timeout
        #   'host=<host>' string is required first to set connect timeout
        #   see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        ybsql_cmd = """ybsql %s "host=%s connect_timeout=%d" <<eof
%s
eof""" % (options, self.env['host'], self.connect_timeout, sql_statement)

        self.set_env(self.env)
        cmd_results = common.call_cmd(ybsql_cmd, stack_level=3)
        self.set_env(self.env_pre)

        return cmd_results

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

        filepath = common.util_dir_path + ('/sql/%s.sql' % stored_proc)
        stored_proc_sql = common.read_file(filepath)

        regex = r"\s*CREATE\s*(OR\s*REPLACE)?\s*PROCEDURE\s*([a-z0-9_]+)\s*\((.*?)\)\s*((RETURNS\s*([a-zA-Z]*).*?))\s+LANGUAGE.+?(DECLARE\s*(.+))?RETURN\s*([^;]*);(.*)\$\$;"
        matches = re.search(regex, stored_proc_sql, re.IGNORECASE | re.DOTALL)

        if not matches:
            common.error("Stored proc '%s' regex parse failed." % stored_proc)

        stored_proc_args          = matches.group(3)
        #TODO currently return_type only handles 1 word like; BOOLEAN
        stored_proc_return_type   = matches.group(6).upper()
        stored_proc_before_return = matches.group(8)
        stored_proc_return        = matches.group(9)
        stored_proc_after_return  = matches.group(10)

        anonymous_block = pre_sql + 'DO $$\nDECLARE\n    --arguments\n'
        if stored_proc_return_type not in ('BOOLEAN', 'BIGINT', 'INT', 'INTEGER', 'SMALLINT'):
            common.error('Unhandled proc return_type: %s' % stored_proc_return_type)
 
        for arg in common.split(stored_proc_args):
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
                    common.error('Unhandled proc arg_type: %s' % arg_type)
            elif default_value:
                anonymous_block += ("    %s %s = %s;\n"
                    % (arg_name, arg_type, default_value))
            else:
                common.error("Missing proc arg: %s for proc: %s"
                    % (arg_name, stored_proc))

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
            # TODO need to figure out howto split the real stderr from stderr RAISE INFO output
            stderr = ''
            stdout = cmd_results.stdout
            stdout_lines = []
            for line in cmd_results.stderr.split('\n'):
                if line[0:20] == 'INFO:  >!>RETURN<!<:':
                    return_value = line[20:].strip()
                elif line[0:7] == 'INFO:  ':
                    stdout_lines.append(line[7:])
                else:
                    stdout_lines.append(line)
            stdout += '\n'.join(stdout_lines)

            if not return_value:
                common.error(cmd_results.stderr)

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
                common.error("Unhandled proc return_type: %s" % stored_proc_return_type)

        return cmd_results

def convert_arg_line_to_args_old(line):
#TODO delete in the future once the new convert_arg_line_to_args proves itself
    for arg in shlex.split(line):
        if not arg.strip():
            continue
        if arg[0] == '#':
            break
        yield arg

def convert_arg_line_to_args(line):
    """This function overrides the convert_arg_line_to_args from argparse.
    It enhances @arg files to have;
        - # comment lines
        - multiline arguments using python style triple double quote notation(in_hard_quote)
    """
    if line[0] == '#': # comment line skip
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
                        dollar_str = ''
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
# Example: yb_common.py -h YB14 -U denav -D denav --verbose 3
if __name__ == "__main__":
    common = common('Common Standalone Debug Run', 'object')
    common.args_add_positional_args()
    common.args_add_optional()
    common.args_add_connection_group()
    common.args_add_filter_group()
    common.args_process()

    if common.verbose >= 3:
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
