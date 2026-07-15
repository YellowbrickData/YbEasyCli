/*

# What is this?

This is a self-contained, no-dependencies, standalone SQL that you can use for extracting all WLM rules
for a specific profile in reusable SQL format. Key difference from "Export SQL" feature in Yellowbrick Manager
is that you can specify your own sorting of the rules and their individual attributes.

# Features

- Arbirary sorting of the rules. The default sorting order is:
	- Profile name (global rules with profile NULL will go on top)
	- Type (submit, compile, etc)
	- Priority (rule execution order)
	- Superuser
	- Enabled
	- Name
- Arbitrary sorting of rule attributes. The default sorting is:
	- Profile name is first
	- Javascript definition is last
	- JS is preceded by System attribute (which is informational only as it's not a valid parameter for CREATE WLM RULE)
	- Everything else (between the explicitly specified items) is sorted by name

# Non-features

- The script does not export the profile itself
- It also does not export pool definitions

# Caveats

Rules are displayed with RAISE INFO in a loop within an anonymous block, so the overall output is not pretty, it has some noise.
I had to add some silly dirty hacks (like printing "skip this") so the noise can be discarded (see usage examples below).

Why is this script so primitive, why doesn't it use nice features like \if conditions etc?
That's because it's primarily aimed to be used with ybsql, which is based on ancient psql 9.6, which doesn't support all those features.

# Usage

The script is supposed to be run with ybsql and requires two input parameters:

1. Profile name
2. Filter condition to extract only specific rules

If you need to change the ordering of rules/attributes, you will need to modify the script itself,
just change _rule_order and/or _wlm_attr_sort variables in DECLARE section.

You can even copy/paste the whole block to DBeaver, change the two vars.* variables accordingly and run the block.
You will have to Ignore the dialog DBeaver shows to get a variable value (it tries being helpful with profile = $1, but we don't need that).

# Usage examples

Get all rules for a profile (including global ones):

ybsql -XAqt -U ybuser -f export-wlm-profile.sql -v v_profile=myprofile -v v_filter=true 2>&1 | grep -v "^ybsql:"

Get only regular user rules local to the profile:

ybsql -XAqt -U ybuser -f export-wlm-profile.sql -v v_profile=myprofile -v v_filter='not superuser and profile is not null' 2>&1 | grep -v "^ybsql:"

*/

set vars.profile to :'v_profile';
set vars.filter to :'v_filter';

DO $block$
DECLARE
	-- NOTE: You can change rule and attribute sort order below.

	-- Define the order in which WLM rules would be extracted/displayed.
	_rule_order text := 'profile NULLS FIRST, type, priority, superuser DESC, enabled, name';
	-- Or use a different order (by rule execution order for example):
	-- _rule_order text := 'DECODE("type", ''submit'', 10, ''assemble'', 20, ''compile'', 30, ''hinting'', 40, ''restart_for_error'', 50, ''restart_for_user'', 60, ''runtime'', 70, ''completion'', 80, 100) AS type_order, priority, enabled';
	-- Define WLM rule atributes sorting order in the final rule definiton.
	-- profile will be on top, javascript on the bottom, anything between (without explicit sort) will be sorted by name.
	_wlm_attr_sort json := '{"profile": 1, "javascript": -1, "system": -2}';

	-- NOTE: DO NOT CHANGE ANYTHING BELOW THIS LINE --------------------------------------------------------------------------------

	-- Define the main SQL for extracting WLM rule info
	-- NOTE: make sure column names match WLM rule attribute names (i.e. priority as rule_order)
	_sql text := $sql$
		SELECT profile, type, name, priority AS rule_order, enabled, superuser, javascript, system
		FROM sys.wlm_classification_rule
		WHERE (profile = $1 OR profile IS NULL)
			AND COALESCE(activated, deactivated) IS NULL
	$sql$;
	_profile text := current_setting('vars.profile');
	_filter text  := current_setting('vars.filter');
	_rec record;
	_attr record;
	_rule text[];
	_txt text;
BEGIN
	FOR _rec IN EXECUTE _sql || ' AND ' || _filter || ' ORDER BY ' || _rule_order USING _profile LOOP
		_rule := '{}';
		FOR _attr IN
			WITH
				o AS (
					SELECT j."key" AS sort_key
						, j."value"::integer AS sort_order
					FROM json_each_text(_wlm_attr_sort) AS j
				),
				x AS (
					SELECT a."key" AS attr_name
						, CASE a."key" WHEN 'javascript' THEN _rec.javascript ELSE a.value END AS attr_value
						, NVL(SIGN(o.sort_order), 0) AS bucket
						, o.sort_order
					FROM json_each(to_json(_rec)) AS a
						LEFT JOIN o ON o.sort_key = a."key"
					WHERE a."key" != 'name'
				)
			SELECT bucket, sort_order
				, CASE attr_name WHEN 'system' THEN '-- ' ELSE '' END || UPPER(attr_name) AS attr_name
				, CASE attr_name WHEN 'javascript' THEN '$$'||attr_value||'$$' ELSE attr_value END AS attr_value
			FROM x
			ORDER BY bucket DESC, sort_order, attr_name
		LOOP
			_rule := array_append(_rule, format(chr(9)||'%-12s %s', _attr.attr_name, COALESCE(_attr.attr_value, 'NULL')));
		END LOOP;
		_txt := '-- just to skip this' || chr(10)
			|| format('CREATE WLM RULE %I (', _rec.name) || chr(10)
			|| array_to_string(_rule, ',' || chr(10))
			|| chr(10) || ');' || chr(10);
		raise warning '%', _txt;
	END LOOP;
END $block$;
