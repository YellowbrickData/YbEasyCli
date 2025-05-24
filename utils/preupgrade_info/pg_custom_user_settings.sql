-- pg_custom_user_settings.sql
-- 2025-03-27

WITH settings AS
(  SELECT
   usename            AS usename
 , unnest (useconfig) AS setting
FROM pg_user
) 
SELECT
   usename
 , setting
FROM settings
WHERE setting NOT LIKE 'ybd_ldap_external%'
ORDER BY usename, setting
;