SELECT relisshared AND current_database() = 'yellowbrick' AS global_scope
	, oid                                                 AS reloid
	, current_database()                                  AS reldb
	, relnamespace::regnamespace::TEXT                    AS relschema
	, relname
	, pg_total_relation_size(oid)                         AS size_total
	, pg_indexes_size(oid)                                AS size_indexes
	, pg_total_relation_size(reltoastrelid)               AS size_toast
	, pg_size_pretty(pg_total_relation_size(oid))         AS size_total_pretty
FROM pg_class
WHERE relkind = 'r'
	AND relschema IN ('sys','pg_catalog')
	AND (NOT relisshared OR global_scope)
	AND size_total > 0
ORDER BY size_total DESC;