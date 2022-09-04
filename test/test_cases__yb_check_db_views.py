test_cases = [
    test_case(
        cmd='yb_check_db_views.py @{argsdir}/db1 --database_in {db1}'
        , exit_code=0
        , stdout="""-- Running broken view check.
-- 0 broken view/s in database "{db1}".
-- Completed check, found 0 broken view/s in 1 db/s."""
        , stderr='')

    , test_case(
        cmd='yb_check_db_views.py @{argsdir}/db2 --database_in {db2}'
        , exit_code=0
        , stdout="""-- Running broken view check.
-- view: {db2}.dev.broken1_v, sqlstate: 42P01, sqlerrm: relation "dze_db1.Prod.dropped_t" does not exist
-- view: {db2}.dev.broken2_v, sqlstate: 42P01, sqlerrm: relation "dze_db1.Prod.Dropped_v" does not exist
-- view: {db2}.dev."Broken3_v", sqlstate: 42P01, sqlerrm: relation "dze_db1.Prod.dropped_t" does not exist
-- view: {db2}."Prod".broken1_v, sqlstate: 42P01, sqlerrm: relation "dze_db1.dev.dropped_t" does not exist
-- 4 broken view/s in database "{db2}".
-- Completed check, found 4 broken view/s in 1 db/s."""
        , stderr='')
]