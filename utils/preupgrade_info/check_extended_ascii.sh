#!/bin/sh
# 
# Check pg_* and sys_* system tables across the yellowbrick or UTF8 databases 
# for object names (dbs, tables, columns, etc..) containing high bit ASCII chars.
#
# NOTE that this does not check for user table columns holding high bit ascii data.
#
# Inputs:
#   $1 (optl) - "shared" : anlyze only shared (global) catalog tables. 
#               Default is to evaluate only UTF8 databases.
#
# Outputs:
# . Returns 0 or the number of DBS with problem objects
# . Console output of evaluated table columns ending with 'Good' or 'WARN' 
# . "ext_ascii.out.tar.gz" - a tarball generated containing a text file for every
#    problem db object type of the form extascii-<db_name>-<table_name>.<col_name>.txt
#
#   For example, the file 'extascii-db_1-pg_attribute.attname.txt'
#   contains column names (which are represented by rows in "pg_attribute.attname" 
#   containing invalid utf8 chars from the database "db_1".
#
#   Output example for 3 column names containing the Latin character "ó" in their names
#      Divisi -->[ó]<-- n de AFORE
#      Divisi -->[ó]<-- n de tienda
#      Coordinaci -->[ó]<-- n
#
# Prerequisites:
# . Run from the manager node as ybdadmin user.
#
# History:
# . 2026.01.17 - Update invalid char regex to be only invalid utf8 chars.
# . 2026.01.13 - Minor formatting and comments.
# . 2025.10.28 - Included in YbEasyCli preupgrade_info.sh
#                Formatting and comments updates.
#
# NOTE: Not tested against "funny" database names (with spaces etc)
#
# TODO:
# . 
#

###############################################################################
# CONSTANTS
###############################################################################

bad_utf8_char_regex='[\xC0-\xC1\xF5-\xFF]|\xE0[\x80-\x9F]|\xED[\xA0-\xBF]|\xF0[\x80-\x8F]|\xF4[\x90-\xBF]'


###############################################################################
# FUNCTIONS
###############################################################################

function get_columns() {
#------------------------------------------------------------------------------
# Generate SQL to get a database's catalog table columns 
#
# Args: 
#   $1 - The database to query
# Uses:
#   TMPFILE - pregenerated tempfile name.
# Affects:
#   SHARED  - Include shared (a.k.a. global) tables flag.
# Outputs:
#   Generated SQL to TMPFILE
#------------------------------------------------------------------------------

  [[ $1 == yellowbrick ]] && SHARED=true || SHARED=false
  
  ybsql -XAqt -d yellowbrick<<TBL_COLS_TO_CHECK_SQL > $TMPFILE || { echo "Failed query for db $1" ; exit $? ; }
  SELECT 
    a.oid::regclass   AS tname, 
    /*b.atttypid::regtype AS ctype,*/ 
    b.attname         AS cname
  FROM       pg_class     AS a
  INNER JOIN pg_attribute AS b ON a.oid = b.attrelid
  WHERE a.relisshared = $SHARED
    AND a.relkind = 'r' /* regular table */
    AND b.atttypid::regtype IN ('name', 'text')
    AND a.relname NOT LIKE '|_log|_%' ESCAPE '|'
    AND a.relnamespace::regnamespace::text IN ('pg_catalog', 'sys')
  ORDER BY a.relname;
TBL_COLS_TO_CHECK_SQL
}


###############################################################################
# BODY
###############################################################################
export YBDATABASE=yellowbrick

[[ "$1" == "shared" ]] && SCOPE=$1 || SCOPE=all
echo Current scope is: $SCOPE catalog tables
# If scope is shared, analyze only the yellowbrick database
[[ $SCOPE == shared ]] && ENC=exclude || ENC=UTF8

TMPFILE=$(mktemp)
# Always process yellowbrick database first and regardless of scope
DBS_SQL="WITH a AS (SELECT 0 AS o, 'yellowbrick' AS name 
  UNION ALL SELECT 1, name FROM sys.database WHERE encoding = '$ENC') 
  SELECT name FROM a ORDER BY o, name"

for DB in $(ybsql -XAqt -c "$DBS_SQL")
do
  echo "Checking database $DB"
  get_columns $DB
  while IFS='|' read TNAME CNAME
  do
    OUTFILE=extascii-$DB-$TNAME.$CNAME.txt
    echo -e "\tChecking $TNAME.$CNAME"
    ybsql -XAqt -d $DB -o $OUTFILE<<HIGH_BIT_COL_NAMES_SQL
    SELECT regexp_replace("$CNAME", '(${bad_utf8_char_regex})', ' -->[\1]<-- ') AS highlight
    FROM $TNAME
    WHERE "$CNAME" ~ '${bad_utf8_char_regex}';
HIGH_BIT_COL_NAMES_SQL
    # Remove the output file for the database if of size Zero. 
    [[ -s $OUTFILE ]] || rm $OUTFILE
  done < $TMPFILE
  rm $TMPFILE
done

# The number of remaining database files will be the count of dbs with problems.
EXT_ASCII_DBS_FOUND=$(find . -type f -name extascii-\*.txt | wc -l)
if [ $EXT_ASCII_DBS_FOUND -gt 0 ] 
then
  echo -e "\033[91mWARN\033[0m: extended ascii found in some system tables"
  tar acvf ext_ascii.out.tar.gz extascii-*.txt
  rm extascii-*.txt
else
  echo -e "\033[92mGOOD\033[0m: no extended ascii found in the system tables"
fi
    
exit $EXT_ASCII_DBS_FOUND