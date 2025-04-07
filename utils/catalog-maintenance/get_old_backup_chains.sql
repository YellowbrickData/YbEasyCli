-- Create a temporary holding space for backup chains/snapshots info
CREATE TEMP TABLE z__tmp_backup_chains (
	db_id                BIGINT,
	db_name              VARCHAR(128),
	db_hot_standby       BOOLEAN,
	last_bck_name        VARCHAR(128),
	last_bck_created     TIMESTAMP,
	last_bck_age         INTEGER,
	last_bck_not_found   BOOLEAN,
	last_rlb_name        VARCHAR(128),
	last_rlb_created     TIMESTAMP,
	last_rlb_age         INTEGER,
	last_rlb_not_found   BOOLEAN,
	in_progress_bck_name VARCHAR(128),
	repl_name            VARCHAR(128),
	chain_repl           BOOLEAN,
	chain_name           VARCHAR(128),
	chain_created        TIMESTAMP,
	chain_age            INTEGER,
	chain_type           VARCHAR(32),
	chain_lock           VARCHAR(16)
);

\pset fieldsep '\t'
\pset tuples_only on
\pset format unaligned
\pset null '\\N'
SELECT to_char(now(), 'YYYYmmdd_HH24MISS') AS ts
\gset

-- Get full report on all backup chains in machine-readable format
\o backup_chains_:ts.mr.out.txt
\i get_backup_chains.sql
\o

-- Load the report into the temp table for further processing
\set copy '\\copy z__tmp_backup_chains from backup_chains_' :ts '.mr.out.txt'
:copy

\pset tuples_only off
\pset format aligned
\pset null ''

\set snapshot_age 30

\qecho == Old (>= :snapshot_age days) backup chains list
-- Select only backup chains with last snapshot older than the defined threshold
SELECT db_name, chain_name, chain_created, chain_age
	, CASE chain_repl WHEN true THEN '+' ELSE '-' END AS chain_repl -- for readability
	, chain_type, last_bck_name, last_bck_created, last_bck_age
FROM z__tmp_backup_chains
WHERE last_bck_age >= :snapshot_age
ORDER BY db_name, chain_age;

\qecho == Old (>= :snapshot_age days) backup chains summary
-- Show summary for all backup chain types on the cluster
SELECT chain_type, Min(last_bck_age) AS min_snapshot_age, Max(last_bck_age) AS max_snapshot_age, Count(*) AS total_chains
	, Sum(CASE WHEN last_bck_age >= :snapshot_age THEN 1 ELSE 0 END) AS old_chains
FROM z__tmp_backup_chains
GROUP BY chain_type
ORDER BY chain_type;

-- Finally, save the full report in human-readable format
\o backup_chains_:ts.hr.out.txt
SELECT * FROM z__tmp_backup_chains WHERE TRUE ORDER BY db_name, chain_age; -- to fool WLM rules that restrict queries without WHERE clause
\o

-- Generate DROP BACKUP CHAIN statements but don't tell the user!
\pset tuples_only on
\pset format unaligned
\o drop_old_backup_chains.out.sql
-- Additional safety measure - immediately exit if someone runs the generated script by mistake
\qecho '\\q'
SELECT CASE row_number() OVER (PARTITION BY db_name ORDER BY chain_name) WHEN 1 THEN '\c '||quote_ident(db_name)||chr(10) ELSE '' END
	||'DROP BACKUP CHAIN '||quote_literal(chain_name)||' CASCADE; -- type: '||chain_type||', last snapshot age: '||last_bck_age
FROM z__tmp_backup_chains
WHERE last_bck_age >= :snapshot_age
ORDER BY db_name, chain_name;
\pset tuples_only off
\pset format aligned
\o
