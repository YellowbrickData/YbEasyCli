WITH
a AS ( -- TODO: add pg_attribute for column-level ACLs (?)
    SELECT oid, 'pg_database'::TEXT    AS objtype, datname             AS objname, 'datacl'::TEXT    AS acl_attr, datacl    AS acl, array_length(datacl, 1)    AS alen FROM pg_database -- WHERE datname = current_database()
    UNION ALL
    SELECT oid, 'pg_namespace'::TEXT   AS objtype, nspname             AS objname, 'nspacl'::TEXT    AS acl_attr, nspacl    AS acl, array_length(nspacl, 1)    AS alen FROM pg_namespace
    UNION ALL
    SELECT oid, 'pg_class'::TEXT       AS objtype, oid::regclass::TEXT AS objname, 'relacl'::TEXT    AS acl_attr, relacl    AS acl, array_length(relacl, 1)    AS alen FROM pg_class
    UNION ALL
    SELECT oid, 'pg_proc'::TEXT        AS objtype, proname             AS objname, 'proacl'::TEXT    AS acl_attr, proacl    AS acl, array_length(proacl, 1)    AS alen FROM pg_proc
    UNION ALL
    SELECT oid, 'pg_default_acl'::TEXT AS objtype, null                AS objname, 'defaclacl'::TEXT AS acl_attr, defaclacl AS acl, array_length(defaclacl, 1) AS alen FROM pg_default_acl
),
b AS (
    SELECT a.oid
        , a.objtype
        , a.objname
        , a.acl_attr
        , u.aclid
-- NOTE: Have to cast aclitem to a primitive datatype, as 5.4.x now barfs on it:
-- "Some of the datatypes only support hashing, while others only support sorting."
        , u.aclitem::text AS acltext
        , acl.grantor AS grantor_id
        , acl.grantee AS grantee_id
        , a.alen
        , string_agg(acl.privilege_type, ',') AS privs
    FROM a
        , unnest(a.acl) WITH ORDINALITY u(aclitem, aclid)
        , aclexplode(ARRAY[u.aclitem]) AS acl
    GROUP BY a.oid
        , a.objtype
        , a.objname
        , a.acl_attr
        , u.aclid
        , acltext
        , acl.grantor
        , acl.grantee
        , a.alen
) -- SELECT * FROM b ORDER BY oid, aclid; -- for debugging
SELECT b.*
    , CASE b.grantor_id WHEN 0 THEN 'public' ELSE granted_by.rolname END AS grantor_name
    , CASE b.grantee_id WHEN 0 THEN 'public' ELSE granted_to.rolname END AS grantee_name
    , format('UPDATE %I SET %I = %s WHERE oid = %s;'
        , b.objtype, b.acl_attr
        , CASE b.alen WHEN 1 THEN 'NULL' ELSE format('array_remove(%I, %I[%s])', b.acl_attr, b.acl_attr, b.aclid) END, b.oid)
      ||CASE WHEN b.objname IS NULL THEN '' ELSE ' -- '||substr(b.objtype, 4)||': '||b.objname END AS sql_fix
FROM b
    LEFT JOIN pg_authid AS granted_by ON granted_by.oid = b.grantor_id
    LEFT JOIN pg_authid AS granted_to ON granted_to.oid = b.grantee_id
WHERE (granted_by.oid IS NULL OR granted_to.oid IS NULL)      -- grantor or grantee is not found
    AND NOT (granted_by.oid IS NOT NULL AND b.grantee_id = 0) -- not something granted by someone to public
ORDER BY b.objtype
	, b.oid
    , b.aclid DESC; -- DESC is very important here as we have to delete array elements in reverse order
