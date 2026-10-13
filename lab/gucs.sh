#!/bin/sh
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.
# Disclaimer:
#   This script does NOT retrieve ALL global (system-level) GUCs as there's no way to do this other than just knowing all their names and querying them one by one.
#   So, until SHOW YBDALL is implemented, the script retrieves ONLY those GUCs that have been explicitly set/overridden at either system or user/role level.

export YBDATABASE=yellowbrick
REPORT=gucs-$(date +%Y%m%d-%H%M%S).txt
while getopts r:d:h:U: OPT ; do
	case $OPT in
		r) REPORT=$OPTARG ;;
		d) export YBDATABASE=$OPTARG ;;
		h) export YBHOST=$OPTARG ;;
		U) export YBUSER=$OPTARG ;;
	esac
done

ybsql -X -o $REPORT <<SQL
\qecho # == Header
WITH v AS (SELECT reverse(split_part(reverse(version()), ' ', 1)) AS ver)
SELECT current_timestamp AS datetime, current_setting('cluster_name') AS "cluster name"
	, inet_server_addr() AS "cluster IP address", v.ver AS "software version", current_database() AS dbname
FROM v;

\qecho # == GUCs defined in config files
SELECT * FROM pg_file_settings ORDER BY seqno;

\qecho # == GUCs defined for roles at database level
SELECT d.oid AS dbid, d.datname AS dbname, r.oid AS roleid, r.rolname AS rolename, UNNEST(dbrs.setconfig) AS roleconfig
FROM pg_db_role_setting dbrs
    JOIN pg_database d ON d.oid = dbrs.setdatabase 
    JOIN pg_roles r ON r.oid = dbrs.setrole 
ORDER BY d.datname, r.rolname, roleconfig;

\qecho # == GUCs defined for roles at system level
SELECT oid AS roleid, rolname AS rolename, UNNEST(rolconfig) AS roleconfig
FROM pg_roles
ORDER BY rolename, roleconfig;
SQL
[ $? -eq 0 ] && echo GUCs info saved into $REPORT file || [ -s $REPORT ] || rm $REPORT
