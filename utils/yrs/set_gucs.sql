-- YB 7.x introduces JSONB datatype, so we have to set a specific GUC to be able to touch tables with JSONB columns (example: yb_metering.staging.instance_event):
DO $block$
BEGIN
  if split_part(current_setting('yb_server_version'), '.', 1)::smallint >= 7 then 
    set enable_full_json to on;
  end if;
END $block$;
