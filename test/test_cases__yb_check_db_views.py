test_cases = [
    test_case(
        cmd='yb_check_db_views.py @{argsdir}/db1 --db_in {db1}'
        , exit_code=0
        , stdout="""-- Running broken view check.
-- 0 broken view/s in "{db1}".
-- Completed check, found 0 broken view/s in 1 db/s."""
        , stderr='')

    , test_case(
        cmd='yb_check_db_views.py @{argsdir}/db2 --db_in {db2}'
        , exit_code=0
        , stdout="""-- Running broken view check.
{db2}.dev.broken1_v
{db2}.dev.broken2_v
{db2}.dev."Broken3_v"
{db2}."Prod".broken1_v
-- 4 broken view/s in "{db2}".
-- Completed check, found 4 broken view/s in 1 db/s."""
        , stderr='')
]