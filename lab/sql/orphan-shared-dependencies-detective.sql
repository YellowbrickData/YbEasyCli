WITH patch_panel AS (
    SELECT 'ACL'       AS tag, 'pg_database'::name AS catalog_ref, 'a'::char AS deptype, 'datacl' AS dep_column, 'datname' AS objname UNION ALL
    SELECT 'ownership' AS tag, 'pg_database'::name               , 'o'                 , 'datdba'              , 'datname'            UNION ALL
    SELECT 'ACL'       AS tag, 'pg_class'::name                  , 'a'                 , 'relacl'              , 'oid::regclass'      UNION ALL
    SELECT 'ownership' AS tag, 'pg_class'::name                  , 'o'                 , 'relowner'            , 'oid::regclass'      UNION ALL
    SELECT 'ACL'       AS tag, 'pg_proc'::name                   , 'a'                 , 'proacl'              , 'oid::regproc'       UNION ALL
    SELECT 'ownership' AS tag, 'pg_proc'::name                   , 'o'                 , 'proowner'            , 'oid::regproc'       UNION ALL
    SELECT 'ACL'       AS tag, 'pg_namespace'::name              , 'a'                 , 'nspacl'              , 'oid::regnamespace'  UNION ALL
    SELECT 'ownership' AS tag, 'pg_namespace'::name              , 'o'                 , 'nspowner'            , 'oid::regnamespace'  UNION ALL
    SELECT 'ACL'       AS tag, 'pg_default_acl'::name            , 'a'                 , 'defaclacl'           , 'oid'  UNION ALL
    SELECT 'ownership' AS tag, 'pg_default_acl'::name            , 'o'                 , 'defaclrole'          , 'oid'
)
, refs AS (
-- Assigning all records with database ID = 0 to 'yellowbrick' database
SELECT sd.refobjid AS roleoid, sd.dbid, NVL(d.datname,'yellowbrick') AS dbname, /*sd.classid AS catalog_oid,*/ c.relname AS catalog_ref, sd.deptype::char AS deptype
    , string_agg(sd.objid, ',')::text AS objids
FROM pg_shdepend          AS sd
         JOIN pg_authid   AS  a ON a.oid = sd.refobjid
    LEFT JOIN pg_database AS  d ON d.oid = sd.dbid
    LEFT JOIN pg_class    AS  c ON c.oid = sd.classid
WHERE sd.objsubid = 0 -- skip column ACLs for now
    AND d.datname = current_database()
GROUP BY sd.refobjid, sd.dbid, d.datname, c.relname, sd.deptype
)
SELECT -- r.roleoid, r.dbname, r.catalog_ref, r.deptype, r.objids, pp.*, -- for debugging purposes
    CASE row_number() OVER (PARTITION BY r.dbname ORDER BY r.catalog_ref, r.deptype, r.roleoid)
        WHEN 1 THEN chr(10)||'\qecho ==== Connecting to '||r.dbname||chr(10)||'\c '||r.dbname
        ELSE ''
    END || chr(10)
    || CASE
        WHEN tag IS NULL THEN '\qecho WARN: Patch Panel configuration not found for '||catalog_ref
        ELSE format('\qecho -- role: %s, checking: %s, catalog table: %s', r.roleoid::regrole, pp.tag, pp.catalog_ref)||chr(10)
        || CASE r.deptype
            WHEN 'o' THEN
                format('SELECT sd.*, ''check -->'' AS chk, d.%s AS objname
    , CASE WHEN d.%s IS NULL THEN ''missing'' ELSE ''ok'' END AS note
FROM pg_shdepend AS sd LEFT JOIN %s AS d ON d.oid = sd.objid'
                    , pp.objname, pp.dep_column, pp.catalog_ref)
            WHEN 'a' THEN
                format('WITH a AS (
    SELECT d.oid, d.%s AS objname, acl.grantor, acl.grantee, acl.privilege_type FROM %s d JOIN aclexplode(d.%s) AS acl ON TRUE WHERE d.oid IN (%s)
) SELECT sd.*, ''check -->'' AS chk
    , CASE WHEN a.grantee IS NULL THEN ''missing'' ELSE format(''%%s granted %%s to %%s on %%s'', a.grantor::regrole, a.privilege_type, a.grantee::regrole, a.objname) END AS note
FROM pg_shdepend AS sd LEFT JOIN a ON a.oid = sd.objid AND a.grantee = sd.refobjid'
                    , pp.objname, pp.catalog_ref, pp.dep_column, r.objids, r.roleoid, r.objids)
            END||chr(10)
        ||format('WHERE sd.refobjid = %s AND sd.objid IN (%s) AND sd.objsubid = 0 AND note = ''missing'';', r.roleoid, r.objids)
    END
    AS sql
FROM refs r LEFT JOIN patch_panel pp USING (catalog_ref, deptype)
ORDER BY r.dbname, r.catalog_ref, r.deptype, r.roleoid;
