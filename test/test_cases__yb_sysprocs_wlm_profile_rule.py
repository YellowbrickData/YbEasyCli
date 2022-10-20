map_out = [ { 'regex' : re.compile(r'(.*)global_mapGCToSystem(.*)', re.DOTALL), 'sub' : 'global_mapGCToSystem' } ]
test_cases = [
    test_case(
        cmd="yb_sysprocs_wlm_profile_rule.py @{argsdir}/db1"
        , exit_code=0
        , stdout="global_mapGCToSystem"
        , stderr=""
        , map_out = map_out )
    , test_case(
        cmd="yb_sysprocs_wlm_profile_rule.py @{argsdir}/db1_su"
        , exit_code=0
        , stdout="global_mapGCToSystem"
        , stderr=""
        , map_out = map_out )
]