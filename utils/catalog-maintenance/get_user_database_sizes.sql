SELECT oid AS db_id, datname AS db_name, pg_database_size(oid) AS db_size_bytes, round(db_size_bytes/1024^2,2) AS db_size_mb
FROM pg_database
WHERE oid > 16000 -- only user databases
ORDER BY db_size_bytes DESC;