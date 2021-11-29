SELECT profile_name                                           AS profile_name
 , rule_name::varchar (256)                                   AS rule_name
 , rule_type                                                  AS rule_type
 , "order"                                                    AS "order"
 , CASE WHEN superuser = 't' THEN 'superuser' ELSE 'user' END AS user_type
 , SUBSTR (expression, 1, strpos (expression, e'\n') -1)      AS description
FROM sys.wlm_active_rule
WHERE (   profile_name IN ( SELECT name FROM sys.wlm_active_profile WHERE active = 't') 
       OR profile_name = '(global)'
      ) AND expression ~ '^//'
ORDER BY "order", rule_type, profile_name
;