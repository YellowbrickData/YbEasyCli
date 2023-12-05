test_cases = [
    test_case(
        cmd="""yb_sysprocs_lock.py @{argsdir}/db1_su"""
        , exit_code=0
        , stdout="""table    database    schema    table    is         lock    sess    sess    sess    sess    sess       sess     b       b       b       b       b          b
id       name        name      name     granted    type    id      user    app     ip      started    state    sess    sess    sess    sess    sess       sess
                                                                                                               id      user    app     ip      started    state

-------  ----------  --------  -------  ---------  ------  ------  ------  ------  ------  ---------  -------  ------  ------  ------  ------  ---------  -------"""
        , stderr="")
]
