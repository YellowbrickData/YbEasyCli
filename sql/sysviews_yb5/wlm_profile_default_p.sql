/* ****************************************************************************
** wlm_profile_default_p()
**
** Creates a Default Profile based on the RAM of a worker blade.
**
** Usage:
**   See COMMENT ON FUNCTION text further below.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2024.03.04 - Created.
*/

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE wlm_profile_default_p(
    -- name of profile to create
    _profile_name                 VARCHAR DEFAULT 'ts_default'
    -- flag when set that drops the old named profile before creating the new profile
    , _drop_old_profile_if_exists BOOLEAN DEFAULT FALSE
    -- flag when set that will activate the newly created profile
    --, _activate_new_profile       BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN 
   LANGUAGE 'plpgsql'
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE
    v_wlm_rec           RECORD;
    v_snippet_rec       RECORD;
    v_wlm_entry         INTEGER := 0;
    v_code              VARCHAR(60000);
    v_pools_js          VARCHAR(60000);
    v_is_active_profile BOOLEAN;
    v_has_load_pool     BOOLEAN;

    v_profile_qry       TEXT := $qry$
        WITH
        profile_by_worker_mem AS (
            SELECT
                0064000 AS worker_mem, 0006000::INT reserved_mem
                , 0::INT AS min_yb_vrsn, 99999::INT AS max_yb_vrsn
                                                                                      -- 0 tmp_pct disables the spill space for that pool
                ,    6::INT AS small_pool_slots,     2048::INT AS small_slot_mib,     0::INT AS small_pool_tmp_pct
                ,    3::INT AS medium_pool_slots,    6656::INT AS medium_slot_mib,    0::INT AS medium_pool_tmp_pct
                ,    1::INT AS large_pool_slots,    (17920::INT - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL::INT AS large_pool_tmp_pct
                ,    0::INT AS load_pool_slots,      NULL::INT AS load_slot_mib,   NULL::INT AS load_pool_tmp_pct
            UNION ALL SELECT
                0128000 AS worker_mem, 0010500 AS reserved_mem
                , 0 AS min_yb_vrsn, 99999 AS max_yb_vrsn
                ,    8 AS small_pool_slots,     2048 AS small_slot_mib,     0 AS small_pool_tmp_pct
                ,    4 AS medium_pool_slots,    8192 AS medium_slot_mib,    0 AS medium_pool_tmp_pct
                ,    2 AS large_pool_slots,    (33280 - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL AS large_pool_tmp_pct
                ,    0 AS load_pool_slots,      NULL AS load_slot_mib,   NULL AS load_pool_tmp_pct
            UNION ALL SELECT
                0256000 AS worker_mem, 0019100 AS reserved_mem
                , 0 AS min_yb_vrsn, 99999 AS max_yb_vrsn
                ,   20 AS small_pool_slots,     2048 AS small_slot_mib,     0 AS small_pool_tmp_pct
                ,    9 AS medium_pool_slots,    8192 AS medium_slot_mib,    0 AS medium_pool_tmp_pct
                ,    4 AS large_pool_slots,    (33024 - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL AS large_pool_tmp_pct
                ,    0 AS load_pool_slots,      NULL AS load_slot_mib,   NULL AS load_pool_tmp_pct
            UNION ALL SELECT
                0512000 AS worker_mem, 0028600 AS reserved_mem
                ,     0 AS min_yb_vrsn, 99999 AS max_yb_vrsn
                ,   30 AS small_pool_slots,     2048 AS small_slot_mib,     0 AS small_pool_tmp_pct
                ,   18 AS medium_pool_slots,    8192 AS medium_slot_mib,    0 AS medium_pool_tmp_pct
                ,    8 AS large_pool_slots,    (32512 - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL AS large_pool_tmp_pct
                ,    6 AS load_pool_slots,      6656 AS load_slot_mib,      1 AS load_pool_tmp_pct
            UNION ALL SELECT
                1024000 AS worker_mem, 0054000 AS reserved_mem
                , 40000 AS min_yb_vrsn, 50399 AS max_yb_vrsn
                ,   50 AS small_pool_slots,     2048 AS small_slot_mib,     0 AS small_pool_tmp_pct
                ,   30 AS medium_pool_slots,    8192 AS medium_slot_mib,    0 AS medium_pool_tmp_pct
                ,   19 AS large_pool_slots,    (33954 - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL AS large_pool_tmp_pct
                ,    6 AS load_pool_slots,      6656 AS load_slot_mib,      1 AS load_pool_tmp_pct
            UNION ALL SELECT
                1024000 AS worker_mem, 0054000 AS reserved_mem
                , 50400 AS min_yb_vrsn, 99999 AS max_yb_vrsn
                ,   58 AS small_pool_slots,     2048 AS small_slot_mib,     0 AS small_pool_tmp_pct
                ,   31 AS medium_pool_slots,    8192 AS medium_slot_mib,    0 AS medium_pool_tmp_pct
                ,   19 AS large_pool_slots,    (32660 - (reserved_mem/large_pool_slots)) AS large_slot_mib,  NULL AS large_pool_tmp_pct
                ,    6 AS load_pool_slots,      6656 AS load_slot_mib,      1 AS load_pool_tmp_pct
        )
        , yb_vrsn AS (
            SELECT
                SPLIT_PART(VERSION(), ' ', 4) AS version
                , SPLIT_PART(version, '-', 1) AS version_number
                , SPLIT_PART(version, '-', 2) AS version_release
                , SPLIT_PART(version_number, '.', 1) AS version_major
                , SPLIT_PART(version_number, '.', 2) AS version_minor
                , SPLIT_PART(version_number, '.', 3) AS version_patch
                , (version_major || LPAD(version_minor, 2, '0') || LPAD(version_patch, 2, '0'))::INT AS version_int
        )
        , worker AS (
            SELECT
                FLOOR(SUM(memory_per_query_bytes * max_concurrency) / (1024^2))       AS actual_worker_mib
                , FLOOR(SUM(temp_space_per_query_bytes * max_concurrency) / (1024^2)) AS actual_worker_tmp_mib
                , FLOOR(SUM(DECODE(TRUE, name = 'system' OR name LIKE '% admin', temp_space_per_query_bytes * max_concurrency, 0) / (1024^2))) AS admin_worker_tmp_mib
                , admin_worker_tmp_mib / actual_worker_tmp_mib * 100 AS admin_worker_tmp_pct
            FROM
                sys.wlm_active_pool
        )
        , worker_profile AS (
            SELECT
                w.actual_worker_mib, w.actual_worker_tmp_mib, v.version_int, p.*
                , small_pool_slots  * small_slot_mib  AS small_pool_mib
                , medium_pool_slots * medium_slot_mib AS medium_pool_mib
                , large_pool_slots  * large_slot_mib  AS large_pool_mib
                , load_pool_slots   * load_slot_mib   AS load_pool_mib
                , 100.0 - (NVL(admin_worker_tmp_pct, 0) + NVL(small_pool_tmp_pct, 0) + NVL(medium_pool_tmp_pct, 0) + NVL(load_pool_tmp_pct, 0)) AS calc_large_pool_tmp_pct
                , FLOOR((NVL(small_pool_tmp_pct, 0)      / 100.0) * actual_worker_tmp_mib) AS small_pool_tmp_mib
                , FLOOR((NVL(medium_pool_tmp_pct, 0)     / 100.0) * actual_worker_tmp_mib) AS medium_pool_tmp_mib
                , FLOOR((NVL(calc_large_pool_tmp_pct, 0) / 100.0) * actual_worker_tmp_mib) AS large_pool_tmp_mib
                , FLOOR((NVL(load_pool_tmp_pct, 0)       / 100.0) * actual_worker_tmp_mib) AS load_pool_tmp_mib
                , DECODE(small_pool_slots,  0, 0, small_pool_tmp_mib  / small_pool_slots)   AS small_slot_tmp_mib
                , DECODE(medium_pool_slots, 0, 0, medium_pool_tmp_mib / medium_pool_slots)  AS medium_slot_tmp_mib
                , DECODE(large_pool_slots,  0, 0, large_pool_tmp_mib  / large_pool_slots)   AS large_slot_tmp_mib
                , DECODE(load_pool_slots,   0, 0, load_pool_tmp_mib   / load_pool_slots)    AS load_slot_tmp_mib
            FROM worker AS w CROSS JOIN yb_vrsn AS v CROSS JOIN profile_by_worker_mem AS p
            ORDER BY ABS((w.actual_worker_mib / p.worker_mem) -1) LIMIT 1
        )
                  SELECT '{actual_worker_mib}' AS alias, 1 AS sub_order, TO_CHAR(actual_worker_mib, '9999999.9') AS code FROM worker_profile
        UNION ALL SELECT '{actual_worker_tmp_mib}',  10 AS sub_order, TO_CHAR(actual_worker_tmp_mib, '9999999.9') AS code FROM worker_profile
        UNION ALL SELECT '{version_int}',            10, TRIM(TO_CHAR(version_int,               '999999'))            FROM worker_profile
        UNION ALL SELECT '{worker_mem}',             10, TRIM(TO_CHAR(worker_mem,                '9999999.9'))         FROM worker_profile
        UNION ALL SELECT '{min_yb_vrsn}',            10, TRIM(TO_CHAR(min_yb_vrsn,               '999999'))            FROM worker_profile
        UNION ALL SELECT '{max_yb_vrsn}',            10, TRIM(TO_CHAR(max_yb_vrsn,               '999999'))            FROM worker_profile
        UNION ALL SELECT '{small_pool_slots}',       10, TRIM(NVL(TO_CHAR(small_pool_slots,      '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{small_pool_mib}',         10, TRIM(NVL(TO_CHAR(small_pool_mib,        '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{small_pool_tmp_pct}',     10, TRIM(NVL(TO_CHAR(small_pool_tmp_pct,    '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{small_pool_tmp_mib}',     10, TRIM(NVL(TO_CHAR(small_pool_tmp_mib,    '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{small_slot_mib}',         10, TRIM(NVL(TO_CHAR(small_slot_mib,        '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{small_slot_tmp_mib}',     10, TRIM(NVL(TO_CHAR(small_slot_tmp_mib,    '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{medium_pool_slots}',      10, TRIM(NVL(TO_CHAR(medium_pool_slots,     '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{medium_pool_mib}',        10, TRIM(NVL(TO_CHAR(medium_pool_mib,       '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{medium_pool_tmp_pct}',    10, TRIM(NVL(TO_CHAR(medium_pool_tmp_pct,   '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{medium_pool_tmp_mib}',    10, TRIM(NVL(TO_CHAR(medium_pool_tmp_mib,   '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{medium_slot_mib}',        10, TRIM(NVL(TO_CHAR(medium_slot_mib,       '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{medium_slot_tmp_mib}',    10, TRIM(NVL(TO_CHAR(medium_slot_tmp_mib,   '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{large_pool_slots}',       10, TRIM(NVL(TO_CHAR(large_pool_slots,      '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{large_pool_mib}',         10, TRIM(NVL(TO_CHAR(large_pool_mib,        '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{large_pool_tmp_pct}',     10, TRIM(NVL(TO_CHAR(large_pool_tmp_pct,    '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{large_pool_tmp_mib}',     10, TRIM(NVL(TO_CHAR(large_pool_tmp_mib,    '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{large_slot_mib}',         10, TRIM(NVL(TO_CHAR(large_slot_mib,        '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{large_slot_tmp_mib}',     10, TRIM(NVL(TO_CHAR(large_slot_tmp_mib,    '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{load_pool_slots}',        10, TRIM(NVL(TO_CHAR(load_pool_slots,       '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{load_pool_mib}',          10, TRIM(NVL(TO_CHAR(load_pool_mib,         '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{load_pool_tmp_pct}',      10, TRIM(NVL(TO_CHAR(load_pool_tmp_pct,     '999'), 'NULL'))      FROM worker_profile
        UNION ALL SELECT '{load_pool_tmp_mib}',      10, TRIM(NVL(TO_CHAR(load_pool_tmp_mib,     '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{load_slot_mib}',          10, TRIM(NVL(TO_CHAR(load_slot_mib,         '99999999'), 'NULL')) FROM worker_profile
        UNION ALL SELECT '{load_slot_tmp_mib}',      10, TRIM(NVL(TO_CHAR(load_slot_tmp_mib,     '99999999'), 'NULL')) FROM worker_profile$qry$;


    v_js_func__pSet TEXT := $JS$function pSet() {
  // set common profile info
  p = {};
  p.small   = '{profile_name}: small';
  p.medium  = '{profile_name}: medium';
  p.large   = '{profile_name}: large';
  p.load    = '{profile_name}: load';
  p.default = p.large;

  p.pool = [];
  p.pool.small  = { slots: {small_pool_slots}, memMib: {small_pool_mib}, spillMib: {small_pool_tmp_mib} };
  p.pool.medium = { slots: {medium_pool_slots}, memMib: {medium_pool_mib}, spillMib: {medium_pool_tmp_mib} };
  p.pool.large  = { slots: {large_pool_slots}, memMib: {large_pool_mib}, spillMib: {large_pool_tmp_mib} };
  p.pool.load   = { slots: {load_pool_slots}, memMib: {load_pool_mib}, spillMib: {load_pool_tmp_mib} };
  for (var pool in p.pool) {
      p.pool[pool].memSlotMib = Math.floor(p.pool[pool].memMib / p.pool[pool].slots);
      p.pool[pool].spillSlotMib = Math.floor(p.pool[pool].spillMib / p.pool[pool].slots);
  }
  p.pool[p.small]  = p.pool.small;
  p.pool[p.medium] = p.pool.medium;
  p.pool[p.large]  = p.pool.large;
  p.pool[p.load]   = p.pool.load;  
}$JS$;

    v_js_rule__set_rule_attr TEXT := $JS$// Description
//     Set rule 'w' attributes
//
// Example Usage
//
//     SELECT /* { "w.priority": "High", "w.memoryEstimate": 1000 } */ 1 FROM sys.const;
//
//     SET ybd_query_tags = '{ "w.priority": "High" }';
//     SELECT 1 FROM sys.const;
//
function main() {
  var queryJSO = getQueryJSO();
  for (attrib in queryJSO) {
    if (attrib == 'w.allowSQB')                       w.allowSQB               = queryJSO[attrib];
    else if (attrib == 'w.application')               w.application            = queryJSO[attrib];
    else if (attrib == 'w.errorRecoverable')       w.errorRecoverable       = queryJSO[attrib];
    else if (attrib == 'w.lookup')                 w.lookup                 = queryJSO[attrib];
    else if (attrib == 'w.maximumExecTimeLimit')   w.maximumExecTimeLimit   = queryJSO[attrib];
    else if (attrib == 'w.maximumRowLimit')        w.maximumRowLimit        = queryJSO[attrib];
    else if (attrib == 'w.maximumWaitLimit')       w.maximumWaitLimit       = queryJSO[attrib];
    else if (attrib == 'w.memoryEstimate')         w.memoryEstimate         = queryJSO[attrib];
    else if (attrib == 'w.priority')               w.priority               = queryJSO[attrib];
    else if (attrib == 'w.memoryRequiredMB')       w.memoryRequiredMB       = queryJSO[attrib];
    else if (attrib == 'w.requestedMemoryMB')      w.requestedMemoryMB      = queryJSO[attrib];
    else if (attrib == 'w.requestedMemoryPercent') w.requestedMemoryPercent = queryJSO[attrib];
    else if (attrib == 'w.requestedSpillMB')       w.requestedSpillMB       = queryJSO[attrib];
    else if (attrib == 'w.requestedSpillPercent')  w.requestedSpillPercent  = queryJSO[attrib];
    else if (attrib == 'w.resourcePool')           w.resourcePool           = queryJSO[attrib];
    else if (attrib == 'w.tags')                   w.tags                   = queryJSO[attrib];
  }
}

// utility functions
{js_func__getQueryJSO}

main();$JS$;


    v_js_rule__log_qry_msg TEXT := $JS$// Description
//     log a message using a query hint or query tag
//
// Example SQL Usage
//
//     SELECT /* { "log_level": "warn", "log_message": "sending a warning to the log" } */ 1 FROM sys.const;
//
//     SET ybd_query_tags = '{ "log_level": "info", "log_message": "sending a info message to the log" }';
//     SELECT 1 FROM sys.const;
//
//     --This example is used to debug a JSON hint/tag when the message is set to 'debug_json'.
//     --    It will log a debug message related to the JSON.
//     SELECT /* { "log_message": "debug_json", "run_in_pool": "run_use_x_lanes": 1.5} */ 1 FROM sys.const;
//
function main() {
  var log_message = getQueryJSONValue("log_message");

  if (log_message) {
    if (log_message == 'debug_json') { debugQueryJSON(); }
    var log_level = getQueryJSONValue("log_level");

    if (log_level)
    {
      if      (log_level == 'info')  log.info(log_message);
      else if (log_level == 'debug') log.debug(log_message);
      else if (log_level == 'warn')  log.warn(log_message);
      else if (log_level == 'error') log.error(log_message);
    }
  }
}

// utility functions
{js_func__getQueryJSONValue}

function debugQueryJSON() {
  var m = 'WLM rule: log_query_message, function: debugQueryJSON\n';

  // first look for hint style JSON name/value like 'SELECT /* {...} */ ...'
  var hintMatchInSQL = (/\/\*\s*(\{.*\})\s*\*\//.exec(w.SQLText));
  if (hintMatchInSQL) {
    m += 'hintMatchInSQL: ' + hintMatchInSQL[1] + '\n';
    try {
      log.debug('Parse Before');
      var hintJSO = JSON.parse(hintMatchInSQL[1]);
      m += 'hintJSONParse: Passed\n';
    }
    catch(err) {
      log.debug('Parse Fail');
      m += 'hintJSONParse: Failed\n';
    }
  }
  else {
    m += 'hintMatchInSQL: None\n';
  }

  // next JSON name/value in SQL tag
  m += 'tag: ' + w.tags + '\n';
  try {
    var tagJSO = JSON.parse(w.tags);
    m += 'tagJSONParse: Passed\n';
  }
  catch(err) {
    m += 'tagJSONParse: Failed\n';
  }

  log.debug(m);
}

main();$JS$;


    v_js_rule__run_qry_in_pool TEXT := $JS$// Description
//     This rule allows an end user to select the pool for a query and/or the
//     number of lanes from the pool to execute a query with.
//     The criteria may be set with the ybd_query_tags setting or with an SQL hint.
//     An SQL hint takes precedence.
//
// Example SQL Usage
//
//     SELECT /* { "runIn": "{profile_name}: large", "runSlots": 1.5 } */ 1 FROM sys.const;
//
//     SET ybd_query_tags = '{ "runIn": "{profile_name}: medium" }';
//     SELECT 1 FROM sys.const;
//
function main() {
  if (w.numRestartError == 0) {

    pSet();

    var runPool = getQueryJSONValue("runIn");
    if (runPool in p.pool) {
      w.resourcePool = runPool;

      var runSlots = getQueryJSONValue("runSlots");
      if (runSlots && !isNaN(runSlots) && runSlots != 1) {
        w.requestedMemoryMB = p.pool[runPool].memSlotMib   * runSlots;
        w.requestedSpillMB  = p.pool[runPool].spillSlotMib * runSlots;
        //log.debug('w.resourcePool: {}, w.requestedMemoryMB: {}, w.requestedSpillMB: {}', w.resourcePool, w.requestedMemoryMB, w.requestedSpillMB);
      }
    }
  }
}

{js_func__pSet}

// utility functions
{js_func__getQueryJSONValue}

main();$JS$;


    v_js_func__getQueryJSO TEXT := $JS$function getQueryJSO(name) {
  var queryJSO = null;

  // first look for hint style JSON name/value like 'SELECT /* {...} */ ...'
  hintMatchInSQL = (/\/\*\s*(\{.*\})\s*\*\//.exec(w.SQLText));
  if (hintMatchInSQL) {
    try {
      queryJSO = JSON.parse(hintMatchInSQL[1]);
    }
    catch(err) { }
  }

  // if no hint style JSON name/value found then check for a JSON name/value in SQL tag
  if (!queryJSO) {
    try {
      queryJSO = JSON.parse(w.tags);
    }
    catch(err) { }
  }

  return(queryJSO);
}$JS$;


    v_js_func__getQueryJSONValue TEXT := $JS$function getQueryJSONValue(name) {
  var value = null;

  // first look for hint style JSON name/value like 'SELECT /* {...} */ ...'
  var hintMatchInSQL = (/\/\*\s*(\{.*\})\s*\*\//.exec(w.SQLText));
  if (hintMatchInSQL) {
    try {
      var hintJSO = JSON.parse(hintMatchInSQL[1]);
      if (name in hintJSO) value = hintJSO[name];
    }
    catch(err) { }
  }

  // if no hint style JSON name/value found then check for a JSON name/value in SQL tag
  if (!value) {
    try {
      var tagJSO = JSON.parse(w.tags);
      if (name in tagJSO) value = tagJSO[name];
    }
    catch(err) { }
  }

  return(value);
}$JS$;


    v_js_func__pushJSORuleToTag TEXT := $JS$function pushJSORuleToTag(ruleObject) {
  var tagsJSO = null;
  try {
    tagsJSO = JSON.parse(w.tags);
    if (!('rule' in tagsJSO)) tagsJSO.rule = [];
  }
  catch(err) {
    if (w.tags === null || w.tags === undefined || w.tags === '') {
        tagsJSO = { "rule": [] };
    }
  }
  if (tagsJSO !== null) {
    tagsJSO.rule.push(ruleObject);
    w.tags = JSON.stringify(tagsJSO);
  }
}$JS$;

BEGIN
    DROP TABLE IF EXISTS wlm_code;
    CREATE TEMP TABLE wlm_code (wlm_entry INTEGER, code VARCHAR(60000));

    DROP TABLE IF EXISTS wlm_snippet;
    CREATE TEMP TABLE wlm_snippet (alias VARCHAR(128), sub_order INTEGER, code VARCHAR(60000));

    SELECT name = _profile_name INTO v_is_active_profile FROM sys.wlm_active_profile WHERE active;

    FOR v_snippet_rec IN EXECUTE v_profile_qry
    LOOP
        INSERT INTO wlm_snippet (alias, sub_order, code)
        VALUES (v_snippet_rec.alias, v_snippet_rec.sub_order, v_snippet_rec.code);
    END LOOP;

    -- this is a bit of a hack to remove load pool JS code lines
    SELECT code != '0' INTO v_has_load_pool FROM wlm_snippet WHERE alias = '{load_pool_slots}';
    IF NOT v_has_load_pool
    THEN
        v_js_func__pset := REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(v_js_func__pset, '[^;]*load[^;]*;', ''), '[^;]*load[^;]*;', ''), '[^;]*load[^;]*;', '');
    END IF;

    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{profile_name}'               , 100, _profile_name);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_rule__set_rule_attr}'     , 1, v_js_rule__set_rule_attr);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_rule__log_qry_msg}'       , 1, v_js_rule__log_qry_msg);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_rule__run_qry_in_pool}'   , 1, v_js_rule__run_qry_in_pool);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_func__pSet}'              , 3, v_js_func__pSet);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_func__getQueryJSO}'       , 2, v_js_func__getQueryJSO);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_func__getQueryJSONValue}' , 2, v_js_func__getQueryJSONValue);
    INSERT INTO wlm_snippet (alias, sub_order, code) VALUES ('{js_func__pushJSORuleToTag}'  , 2, v_js_func__pushJSORuleToTag);

----------------------- Start Profile ------------------------------
-- Profile: {profile_name}
--------------------------------------------------------------------
    IF _drop_old_profile_if_exists
    THEN
        v_wlm_entry := v_wlm_entry + 1;
        INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_PROFILE$
            DROP WLM PROFILE IF EXISTS "{profile_name}";
$WLM_PROFILE$);
    END IF;

    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_PROFILE$
    CREATE WLM PROFILE "{profile_name}" ( DEFAULT_POOL "{profile_name}: large" );
$WLM_PROFILE$);
----------------------- End Profile   ------------------------------


----------------------- Start Pool ---------------------------------
-- Pool: {profile_name}: admin
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
    DROP WLM RESOURCE POOL IF EXISTS "{profile_name}: admin";
    CREATE WLM RESOURCE POOL "{profile_name}: admin"
    (
        PROFILE                      "{profile_name}"
        , MAX_CONCURRENCY            1
        , MIN_CONCURRENCY            1
        , QUEUE_SIZE                 100
        , MAX_SPILL_PCT              '5'
        , REQUESTED_MEMORY           '3900MB'
        , MAXIMUM_WAIT_LIMIT         NULL
        , MAXIMUM_ROW_LIMIT          NULL
        , MAXIMUM_EXEC_TIME          NULL
        , NEXT_MEMORY_QUEUE          NULL
        , NEXT_EXEC_TIME_LIMIT_QUEUE NULL
    );
$WLM_POOL$);
----------------------- End Pool   ---------------------------------


----------------------- Start Pool ---------------------------------
-- Pool: {profile_name}: load
--------------------------------------------------------------------
    IF v_has_load_pool
    THEN
        v_wlm_entry := v_wlm_entry + 1;
        INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
        DROP WLM RESOURCE POOL IF EXISTS "{profile_name}: load";
        CREATE WLM RESOURCE POOL "{profile_name}: load"
        (
            PROFILE                      "{profile_name}"
            , MAX_CONCURRENCY            {load_pool_slots}
            , MIN_CONCURRENCY            {load_pool_slots}
            , QUEUE_SIZE                 2000
            , MAX_SPILL_PCT              '{load_pool_tmp_pct}'
            , REQUESTED_MEMORY           '{load_pool_mib}MB'
            , MAXIMUM_WAIT_LIMIT         NULL
            , MAXIMUM_ROW_LIMIT          NULL
            , MAXIMUM_EXEC_TIME          NULL
            , NEXT_MEMORY_QUEUE          NULL
            , NEXT_EXEC_TIME_LIMIT_QUEUE NULL
        );
$WLM_POOL$);
    END IF;
----------------------- End Pool   ---------------------------------


----------------------- Start Pool ---------------------------------
-- Pool: {profile_name}: large
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
    DROP WLM RESOURCE POOL IF EXISTS "{profile_name}: large";
    CREATE WLM RESOURCE POOL "{profile_name}: large"
    (
        PROFILE                      "{profile_name}"
        , MAX_CONCURRENCY            {large_pool_slots}
        , MIN_CONCURRENCY            {large_pool_slots}
        , QUEUE_SIZE                 1000
        , MAX_SPILL_PCT              NULL
        , REQUESTED_MEMORY           NULL
        , MAXIMUM_WAIT_LIMIT         NULL
        , MAXIMUM_ROW_LIMIT          NULL
        , MAXIMUM_EXEC_TIME          NULL
        , NEXT_MEMORY_QUEUE          NULL
        , NEXT_EXEC_TIME_LIMIT_QUEUE NULL
    );
$WLM_POOL$);
----------------------- End Pool   ---------------------------------


----------------------- Start Pool ---------------------------------
-- Pool: {profile_name}: medium
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
    DROP WLM RESOURCE POOL IF EXISTS "{profile_name}: medium";
    CREATE WLM RESOURCE POOL "{profile_name}: medium"
    (
        PROFILE                      "{profile_name}"
        , MAX_CONCURRENCY            {medium_pool_slots}
        , MIN_CONCURRENCY            {medium_pool_slots}
        , QUEUE_SIZE                 2000
        , MAX_SPILL_PCT              '{medium_pool_tmp_pct}'
        , REQUESTED_MEMORY           '{medium_pool_mib}MB'
        , MAXIMUM_WAIT_LIMIT         NULL
        , MAXIMUM_ROW_LIMIT          NULL
        , MAXIMUM_EXEC_TIME          NULL
        , NEXT_MEMORY_QUEUE          NULL
        , NEXT_EXEC_TIME_LIMIT_QUEUE NULL
    );
$WLM_POOL$);
----------------------- End Pool   ---------------------------------


----------------------- Start Pool ---------------------------------
-- Pool: {profile_name}: small
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
    DROP WLM RESOURCE POOL IF EXISTS "{profile_name}: small";
    CREATE WLM RESOURCE POOL "{profile_name}: small"
    (
        PROFILE                      "{profile_name}"
        , MAX_CONCURRENCY            {small_pool_slots}
        , MIN_CONCURRENCY            {small_pool_slots}
        , QUEUE_SIZE                 2000
        , MAX_SPILL_PCT              '{small_pool_tmp_pct}'
        , REQUESTED_MEMORY           '{small_pool_mib}MB'
        , MAXIMUM_WAIT_LIMIT         NULL
        , MAXIMUM_ROW_LIMIT          NULL
        , MAXIMUM_EXEC_TIME          NULL
        , NEXT_MEMORY_QUEUE          NULL
        , NEXT_EXEC_TIME_LIMIT_QUEUE NULL
    );
$WLM_POOL$);
----------------------- End Pool   ---------------------------------

----------------------- Start Rule 001 -----------------------
-- Superuser: false, Type: submit, Order: 160
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilLogQryMsg";
    CREATE WLM RULE "{profile_name}: utilLogQryMsg"
    (
        PROFILE      "{profile_name}"
        , TYPE       submit
        , RULE_ORDER 160
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilLogQryMsg
{js_rule__log_qry_msg}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 001   -----------------------

----------------------- Start Rule 003 -----------------------
-- Superuser: false, Type: compile, Order: 110
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: abortByDuration";
    CREATE WLM RULE "{profile_name}: abortByDuration"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 110
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: abortByDuration
// Description
//     Example rule of how to terminate long running queries
function main() {
  setTimeout(function () {
    w.errorRecoverable = false;
    log.info('Cancelling long-running query after 24 hours: {}', w.execId);
    w.abort('Cancelling long-running query after 24 hours');
  }, 24*60*60*1000);
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 003   -----------------------

----------------------- Start Rule 004 -----------------------
-- Superuser: false, Type: compile, Order: 130
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: abortByTimeOfDay";
    CREATE WLM RULE "{profile_name}: abortByTimeOfDay"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 130
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: abortByTimeOfDay
// Description
//     Example rule of how to terminate at a certain time of day
function main() {
  var m = new Date().getMinutes();
  var h = new Date().getHours();
  var min_of_day = m + (h * 60);

  // between 12:40 PM and 12:45 PM
  var min_from = (12 * 60) + 40;
  var min_to = (12 * 60) + 45;

  if (min_of_day >= min_from && min_of_day < min_to) {
    w.errorRecoverable = false;
    abort_message = 'Database is queised between ' + min_from + ' and ' + min_to + ' minutes of the day.'
    w.abort(abort_message);
  }
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 004   -----------------------

----------------------- Start Rule 005 -----------------------
-- Superuser: false, Type: compile, Order: 110010
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: poolSmallQry";
    CREATE WLM RULE "{profile_name}: poolSmallQry"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 110010
        , ENABLED    true
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: poolSmallQry
// Description
//     Place small queries in the small pool
function main() {
  var runInSmall = false;
  var highEstMemFactor = 1.50;
  var noneEstMemFactor = 0.25;

  pSet();

  if (
      w.memoryRequiredMB <= p.pool.small.memSlotMib
      && (
        (w.memoryEstimateConfidence    === 'High' && w.memoryEstimate <= (p.pool.small.memSlotMib * highEstMemFactor))
        || (w.memoryEstimateConfidence === 'None' && w.memoryEstimate <= (p.pool.small.memSlotMib * noneEstMemFactor))
      )
      // todo ADD COMMENT
      //&& ( ['update', 'insert', 'delete', 'ctas', 'select'].indexOf(w.type) >= 0 )
  )
  { runInSmall = true; }

  else if (w.type === 'analyze' && w.resourcePool !== p.small)
  { runInSmall = true; }

  else if ( ['drop', 'drop table'].indexOf(w.type) >= 0 )
  { runInSmall = true; }

  else if (
      w.type === 'delete'
      && String(w.SQLText).toLowerCase().indexOf('truncate') >= 0
    )
  { runInSmall = true; }

  if (runInSmall && w.numRestartError == 0) {
    w.resourcePool = p.small;
  }
}

// utility functions
{js_func__pSet}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 005   -----------------------

----------------------- Start Rule 006 -----------------------
-- Superuser: false, Type: compile, Order: 110020
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: poolMediumQry";
    CREATE WLM RULE "{profile_name}: poolMediumQry"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 110020
        , ENABLED    true
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: poolMediumQry
// Description
//     Place medium queries in the medium pool
function main() {
  var runInMedium = false;
  var highEstMemFactor = 1.50;
  var noneEstMemFactor = 0.25;

  pSet();

  if (
      w.memoryRequiredMB <= p.pool.medium.memSlotMib
      && (
        (w.memoryEstimateConfidence    === 'High' && w.memoryEstimate <= (p.pool.medium.memSlotMib * highEstMemFactor))
        || (w.memoryEstimateConfidence === 'None' && w.memoryEstimate <= (p.pool.medium.memSlotMib * noneEstMemFactor))
      )
      // uncomment if you want to only move certain query types to the medium pool
      //&& ( ['update', 'insert', 'delete', 'ctas', 'select'].indexOf(w.type) >= 0 )
  )
  { runInMedium = true; }

  if (runInMedium && w.numRestartError == 0) {
    w.resourcePool = p.medium;
  }
}

// utility functions
{js_func__pSet}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 006   -----------------------

----------------------- Start Rule 007 -----------------------
-- Superuser: false, Type: compile, Order: 110030
--------------------------------------------------------------------
    IF v_has_load_pool
    THEN
        v_wlm_entry := v_wlm_entry + 1;
        INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
        DROP WLM RULE IF EXISTS "{profile_name}: poolLoadQry";
        CREATE WLM RULE "{profile_name}: poolLoadQry"
        (
            PROFILE      "{profile_name}"
            , TYPE       compile
            , RULE_ORDER 110030
            , ENABLED    true
            , SUPERUSER  false
            , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: poolLoadQry
// Description
//     Place load queries in the load pool
function main() {
  var runInLoad = false;
  var highEstMemFactor = 1.50;
  var noneEstMemFactor = 0.25;

  pSet();

  if (
      w.memoryRequiredMB <= p.pool.load.memSlotMib
      && ( ['load', 'ycopy'].indexOf(w.type) >= 0 )
  )
  { runInLoad = true; }

  if (runInLoad && w.numRestartError == 0) {
    w.resourcePool = p.load;
  }
}

// utility functions
{js_func__pSet}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
    END IF;
----------------------- End Rule 007   -----------------------

----------------------- Start Rule 008 -----------------------
-- Superuser: false, Type: compile, Order: 110030
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: poolRestartOnError";
    CREATE WLM RULE "{profile_name}: poolRestartOnError"
    (
        PROFILE      "{profile_name}"
        , TYPE       restart_for_error
        , RULE_ORDER 140
        , ENABLED    true
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: poolRestartOnError
// Description
//     Restart failed queries in the large pool
//         that ran in a non-large pool with a memory error
//
function main() {
  var memoryError = ['53200', 'EEOOM', 'KE002', 'KE029', 'KE032', 'YB004', 'YB005', 'YB006'];

  pSet();

  if (w.resourcePool != p.large
    && w.errorRecoverable
    && memoryError.indexOf(String(w.errorCode)) >= 0) {

    log.info('Restart query: ' + w.execId + ' that failed in "' + w.resourcePool + '" pool due to errorCode: ' + w.errorCode);
    log.info('----errorMessage: ' + w.errorMessage);    

    w.restartInResourcePool(p.large);
  }
}

// utility functions
{js_func__pSet}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 008   -----------------------

----------------------- Start Rule 009 -----------------------
-- Superuser: false, Type: compile, Order: 110040
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: slotUsageMemoryRequired";
    CREATE WLM RULE "{profile_name}: slotUsageMemoryRequired"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 110040
        , ENABLED    true
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: slotUsageMemoryRequired
// Description
//     This rule minimizes query WL005 errors which are not restartable.
//     When a query runs in a slot that that has less memory than w.memoryRequiredMB
//         it will fail with a WL005 error.
//     When the slot has less memory than w.memoryRequiredMB it will attempt to give
//         the query multiple slots worth of memory to satisfy w.memoryRequiredMB.
function main() {
    pSet();

    var currPool        = w.resourcePool ? p.pool[w.resourcePool] : p.pool[p.default];
    var maxSlotsAllowed = Math.floor(currPool.slots / 2);

    if (w.resourcePool != p.small && w.memoryRequiredMB > currPool.memSlotMib ) {
        // Determine how many "slots" from the pool are needed to satisfy w.memoryRequiredMB
        var slots = Math.min(maxSlotsAllowed, (Math.ceil(w.memoryRequiredMB/currPool.memSlotMib)));
        if (slots > 1) log.warn('Query {} has been granted {} slots in the "{}" pool'
            , w.execId, slots, w.resourcePool);

        var memMB   = slots * currPool.memSlotMib;
        var spillMB = slots * currPool.spillSlotMib;

        if (w.requestedMemoryMB < memMB)   w.requestedMemoryMB = Math.floor(memMB);
        if (w.requestedSpillMB  < spillMB) w.requestedSpillMB  = Math.floor(spillMB);
    }
}

// utility functions
{js_func__pSet}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 009   -----------------------


----------------------- Start Rule 010 -----------------------
-- Superuser: false, Type: compile, Order: 130010
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: priorityByQryType";
    CREATE WLM RULE "{profile_name}: priorityByQryType"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 130010
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: priorityByQryType
// Description
//     Example rule that sets query priority by query type
function main() {
  var low_priority_types = ['load', 'unload', 'ycopy']

  if (low_priority_types.indexOf(w.type) >= 0) {
    w.priority = 'low';
  }
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 010   -----------------------


----------------------- Start Rule 011 -----------------------
-- Superuser: false, Type: compile, Order: 130020
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: priorityByUserAndRole";
    CREATE WLM RULE "{profile_name}: priorityByUserAndRole"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 130020
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: priorityByUserAndRole
// Description
//     Example rule that sets query priority by user name/role
function main() {
  var user = String(w.user).toLowerCase();
  var roles = String(w.roles).toLowerCase().split(',');

  var critical_users = ['etluser', 'dbauser'];
  var low_users = ['user_name_1', 'user_name_2'];
  var low_roles = ['dev'];
  var low_dev_users = ['dev_user_name_1', 'dev_user_name_2', 'dev_user_name_3'];
  var low_dev_roles = ['dev', 'dev_etl', 'uat', 'uat_etl'];

  if (user.indexOf(critical_users) >= 0)
  { w.priority = 'critical'; }

  else if (user.indexOf(low_users) >= 0)
  { w.priority = 'low'; }

  else if (user.indexOf(low_dev_users) >= 0)
  { w.priority = 'low'; }

  else if (hasIntersect(roles, low_roles))
  { w.priority = 'low'; }

  else if (hasIntersect(roles, low_dev_roles))
  { w.priority = 'low'; }
}

// utility functions
function hasIntersect(a, b) {
  for (x in a) {
    if (x.indexOf(b) !== -1) return true;
  }
  return false;
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 011   -----------------------


----------------------- Start Rule 012 -----------------------
-- Superuser: false, Type: compile, Order: 140010
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilRunQryInPool";
    CREATE WLM RULE "{profile_name}: utilRunQryInPool"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 140010
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilRunQryInPool
{js_rule__run_qry_in_pool}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 012   -----------------------

----------------------- Start Rule 013 -----------------------
-- Superuser: false, Type: compile, Order: 140020
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilSetRuleAttr";
    CREATE WLM RULE "{profile_name}: utilSetRuleAttr"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 140020
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilSetRuleAttr
{js_rule__set_rule_attr}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 013   -----------------------


----------------------- Start Rule 014 -----------------------
-- Superuser: false, Type: compile, Order: 160010
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: overrideGlobal_restartErrorPolicy";
    CREATE WLM RULE "{profile_name}: overrideGlobal_restartErrorPolicy"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 160010
        , ENABLED    true
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: overrideGlobal_restartErrorPolicy
// Description
//     The gloabal rule sets queries to restart 3 times on failure.
//     The global restart policy has value for corner case queries only.
//     This rule overrides the globel rule and properly handles query restarts.
function main() {
  var restarts_allowed = 0;

  if (w.resourcePool === '{profile_name}: small'
    || w.resourcePool === '{profile_name}: medium'
    || w.resourcePool === '{profile_name}: load') {
    restarts_allowed = 1;
  }

  w.errorRecoverable = w.numRestartError < restarts_allowed;
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 014   -----------------------

----------------------- Start Rule 015 -----------------------
-- Superuser: false, Type: runtime, Order: 120
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: abortByDurationAndUser";
    CREATE WLM RULE "{profile_name}: abortByDurationAndUser"
    (
        PROFILE      "{profile_name}"
        , TYPE       runtime
        , RULE_ORDER 120
        , ENABLED    false
        , SUPERUSER  false
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: abortByDurationAndUser
// Description
//     Example rule that aborts queries by user and duration
function main() {
  var user = String(w.user).toLowerCase();
  var abort_users = ['user_name_1', 'user_name_2', '...'];
  var abort_seconds = 100

  // If running for more than 100 seconds and in abort_users then abort query
  if (w.executionDuration > (abort_seconds * 1000)
    && abort_users.indexOf(user) >= 0) {
    w.errorRecoverable = false;
    abort_message = 'Cancelling long-running query after ' + abort_seconds + ' second'
    w.abort(abort_message);
  }
}

main();$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 015   -----------------------

----------------------- Start Rule 016 -----------------------
-- Superuser: true, Type: submit, Order: 150
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilLogQryMsgSU";
    CREATE WLM RULE "{profile_name}: utilLogQryMsgSU"
    (
        PROFILE      "{profile_name}"
        , TYPE       submit
        , RULE_ORDER 150
        , ENABLED    true
        , SUPERUSER  true
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilLogQryMsgSU
{js_rule__log_qry_msg}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 016   -----------------------

----------------------- Start Rule 017 -----------------------
-- Superuser: true, Type: compile, Order: 150010
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilRunQryInPoolSU";
    CREATE WLM RULE "{profile_name}: utilRunQryInPoolSU"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 150010
        , ENABLED    true
        , SUPERUSER  true
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilRunQryInPoolSU
{js_rule__run_qry_in_pool}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 017   -----------------------

----------------------- Start Rule 018 -----------------------
-- Superuser: true, Type: compile, Order: 150020
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{profile_name}: utilSetRuleAttrSU";
    CREATE WLM RULE "{profile_name}: utilSetRuleAttrSU"
    (
        PROFILE      "{profile_name}"
        , TYPE       compile
        , RULE_ORDER 150020
        , ENABLED    true
        , SUPERUSER  true
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS$// Rule: utilSetRuleAttrSU
{js_rule__set_rule_attr}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule 018   -----------------------

    FOR v_wlm_rec IN SELECT code FROM wlm_code ORDER BY wlm_entry
    LOOP
        v_code := v_wlm_rec.code;

        FOR v_snippet_rec IN SELECT alias, code FROM wlm_snippet ORDER BY sub_order
        LOOP
            v_code := REPLACE(v_code, v_snippet_rec.alias, v_snippet_rec.code);
        END LOOP;

        --RAISE INFO '%', v_code; --DEBUG
        EXECUTE v_code;
    END LOOP;

--    COMMIT;

    --The activation in a stored proc doesn't seem to work
    --IF _activate_new_profile OR v_is_active_profile
    --THEN
    --    EXECUTE FORMAT('ALTER WLM PROFILE "%s" ACTIVATE 10 WITHOUT CANCEL', _profile_name);
    --END IF;

    RETURN TRUE;
END
$proc$ 
;


COMMENT ON FUNCTION wlm_profile_default_p( _profile_name VARCHAR, _drop_old_profile_if_exists BOOLEAN ) IS 
$str$Description:
Creates a Default Profile based on the RAM of a worker blade, returns TRUE on success.
  
Examples:
  SELECT * FROM wlm_profile_default_p();
  SELECT * FROM wlm_profile_default_p('my_default_profile', FALSE, TRUE);

Arguments: 
. _profile_name               - VARCHAR - name of profile to create - DEFAULT 'ts_default'
. _drop_old_profile_if_exists - BOOLEAN - drop the old named profile before creating the new profile - DEFAULT FALSE

Version:
. 2024.03.04 - Yellowbrick Technical Support 
$str$
;