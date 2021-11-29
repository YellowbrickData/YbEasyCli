/* sysviews_settings.sql
**
** Create, populate, update the sysviews_settings table which holds configurable
** sysviews default settings.
**
** Revision History:
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.04.25 - Yellowbrick Technical Support 
** 
*/

CREATE TABLE IF NOT EXISTS sysviews_settings
   (
      name        VARCHAR (128)
    , superuser   BOOLEAN
    , setting     VARCHAR (128)
    , max_setting VARCHAR (128)
   )
;

/* Set default values for max_text_len for non-superusers and superusers
*/
INSERT INTO sysviews_settings
SELECT
   name
 , superuser
 , setting
 , max_setting
FROM
   (  SELECT
         0              AS precedence
       , 'max_text_len' AS name
       , 'f'            AS superuser
       , '32'           AS setting
       , '32'           AS max_setting
      FROM
         sys.const
      UNION ALL
      SELECT
         1           AS precedence
       , name        AS name
       , superuser   AS superuser
       , setting     AS setting
       , max_setting AS max_setting
      FROM
         sysviews_settings
      WHERE
         name = 'max_text_len'
      ORDER BY
         precedence DESC
      LIMIT 1
   ) sq
;

INSERT INTO sysviews_settings
SELECT
   name
 , superuser
 , setting
 , max_setting
FROM
   (  SELECT
         0              AS precedence
       , 'max_text_len' AS name
       , 't'            AS superuser
       , '32'           AS setting
       , '64000'        AS max_setting
      FROM
         sys.const
      UNION ALL
      SELECT
         1           AS precedence
       , name        AS name
       , superuser   AS superuser
       , setting     AS setting
       , max_setting AS max_setting
      FROM
         sysviews_settings
      WHERE
         name = 'max_text_len'
      ORDER BY
         precedence DESC
      LIMIT 1
   ) sq
;