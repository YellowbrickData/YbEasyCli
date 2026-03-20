-- set_gucs.sql
-- 
-- YB 7.x introduces JSONB datatype which is used in in some system tables.
-- However, the option is not on be default for statements executed by users. 
-- Enable it so that tables with JSONB columns can be flushed. 
-- Example: yb_metering.staging.instance_event):

DO $block$
BEGIN
  IF split_part(current_setting('yb_server_version'), '.', 1)::SMALLINT >= 7 THEN 
    SET enable_full_json TO ON;
  END IF;
END $block$;
