#!/bin/sh
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.
export YBDATABASE=yellowbrick
PGDATA=/mnt/ybdata/ybd/postgresql/build/db/data
DBSIZE=500
FNSIZE=100
while getopts d:s:f:h OPT ; do
	case $OPT in
		d) export YBDATABASE=$OPTARG ;;
		s) DBSIZE=$OPTARG ;;
		f) FNSIZE=$OPTARG ;;
		h) echo "Usage: $(basename $0) [-d database, default: $YBDATABASE] [-s database_size_mb_filter, default: $DBSIZE] [-f file_size_mb_filter, default: $FNSIZE]" ; exit ;;
	esac
done
[ $YBDATABASE = 'yellowbrick' ] && CATALOG=global || CATALOG=base/$(ybsql -XAqt -d yellowbrick -c "SELECT database_id FROM sys.database WHERE name = '$YBDATABASE'")
[ $CATALOG = 'base/' ] && { echo ERROR: could not find \'$YBDATABASE\' database ID; exit 1 ; }
sudo test -d $PGDATA/$CATALOG || { echo ERROR: could not find $PGDATA/$CATALOG directory ; exit 1 ; }

echo -e "PG catalog mapper v0.4b\nPGDATA: $PGDATA"

TEMPDIR=/tmp/fsinfo-$(uuidgen)
mkdir $TEMPDIR
cd $TEMPDIR

sudo sh -c "cd $PGDATA ; du -b -d 1 base global | sort -rn">dbfs.txt
ybsql -XAqt -F $'\t' -o dbinfo.txt -c 'SELECT database_id, name FROM sys.database'

sudo find $PGDATA/$CATALOG -type f -printf '%f\t%s\n'>fsinfo.txt
ybsql -XAqt -F $'\t' -o pgcat.txt<<SQL
SELECT c.oid, s.nspname||'.'||c.relname AS relfqname, c.relfilenode, reltuples, c.relkind, c.relcreated
        , pg_relation_filepath(c.oid) AS fpath, pg_relation_filenode(c.oid) AS fnode
        , pg_table_size(c.oid) AS table_size, pg_indexes_size(c.oid) AS indexes_size
FROM pg_class c
	JOIN pg_namespace s ON s.oid = c.relnamespace
WHERE s.nspname IN ('sys', 'pg_catalog')
SQL

[ $YBDATABASE = yellowbrick ] && DBHEADER="$YBDATABASE (a.k.a. \'global catalog\')" || DBHEADER="$YBDATABASE (a user database)"

sqlite3 data.db<<SQL
CREATE TABLE dbfs (sizeb INTEGER, dir TEXT);
CREATE TABLE dbinfo (id INTEGER, name TEXT);
CREATE TABLE fsinfo (filenode TEXT, sizeb INTEGER);
CREATE TABLE pgcat (oid INTEGER, relfqname TEXT, relfilenode INTEGER, rowcount INTEGER, relkind CHAR(1), created TIMESTAMP, fpath TEXT, fnode INTEGER, osize INTEGER, isize INTEGER);

.separator \t
.import dbfs.txt dbfs
.import dbinfo.txt dbinfo
.import fsinfo.txt fsinfo
.import pgcat.txt pgcat

CREATE TEMP TABLE fnagg AS
SELECT substr(filenode, 1, ifnull(nullif(instr(filenode, '.'), 0), 123)-1) AS filenode, sum(sizeb) AS sizeb, count(*) AS chunks
FROM fsinfo
GROUP BY 1;

.head on
.mode column

.print \n\033[36m------------------------------------------------------------------------------------------------------
.print Summary report overview
.print ------------------------------------------------------------------------------------------------------
.print The first two lines don\'t have Database ID and show total sizes for:
.print "  - \'global catalog\', where all shared tables live (sys.shardstore, sys.kri, etc)"
.print "  - \'base\' directory, which contains system catalogs for all user databases"
.print All other lines with non-empty Database ID show total size per each directory under \'base\'
.print These directories represent user databases and contain per-database objects in pg_catalog schema
.print ------------------------------------------------------------------------------------------------------\033[0m

.print \n\033[92m== Summary: database catalog sizes > $DBSIZE MB\033[0m
SELECT b.id AS "Database ID", CASE a.dir WHEN 'base' THEN '<total size of ''base'' directory>' WHEN 'global' THEN '<global catalog>' ELSE b.name END AS "Database name                      |"
	, a.sizeb/1024/1024 AS "^ Catalog size, MB", a.dir AS "Catalog path relative to PGDATA"
FROM dbfs a
	LEFT JOIN dbinfo b ON b.id = substr(a.dir, instr(a.dir,'/')+1)
WHERE a.sizeb > $DBSIZE*1024*1024
ORDER BY a.sizeb DESC;

.print \n\033[92m== Database: $DBHEADER\033[0m

.print \n\033[32m==== File nodes (size > $FNSIZE MB) mapping to PG catalog objects\033[0m
SELECT p.fpath AS "file node in PGDATA            |", f.chunks AS fn_chunks
	, f.sizeb/1024/1024 AS "^ fn_agg_size_mb", p.osize/1024/1024 AS osize_mb, p.isize/1024/1024 AS isize_mb
        , p.oid, CASE p.relkind WHEN 'r' THEN 'table' WHEN 'i' THEN 'index' WHEN 't' THEN 'toast' ELSE p.relkind END AS otype
	, p.relfqname AS "fully qualified relation name                   |", p.rowcount, p.created
FROM fnagg f
        JOIN pgcat p ON p.fpath = '$CATALOG/'||f.filenode
WHERE f.sizeb > $FNSIZE*1024*1024
ORDER BY f.sizeb DESC;

CREATE TEMP TABLE nomap AS
SELECT filenode FROM fnagg
WHERE sizeb > $FNSIZE*1024*1024 AND '$CATALOG/'||filenode NOT IN (SELECT fpath FROM pgcat);

.print \n\033[32m==== File nodes (size > $FNSIZE MB) without mapping\033[0m
SELECT f.filenode AS "file node in PGDATA/$CATALOG", f.sizeb/1024/1024 AS size_mb
FROM fsinfo f
	JOIN nomap n ON f.filenode = n.filenode OR f.filenode LIKE n.filenode||'.%' OR f.filenode LIKE n.filenode||'|_%' ESCAPE '|'
ORDER BY f.filenode;
SQL
cd
echo -e "\n\033[92m== Stray files (size > $FNSIZE MB), not belonging to any database in particular\033[0m"
sudo sh -c "cd $PGDATA ; sudo find . ./base -maxdepth 1 -type f -size +${FNSIZE}M -exec ls -lh {} +"
rm -rf $TEMPDIR
