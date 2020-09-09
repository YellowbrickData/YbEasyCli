test_cases = [
    test_case(
        cmd=(
            """yb_mass_column_update.py @{argsdir}/db1 --datatype_like 'CHAR%%' """
            """--update_where_clause "INSTR(<columnname>, 'a') > 0" """
            """--set_clause "REPLACE(<columnname>, 'a', '')" """)
        , exit_code=0
        , stdout="""-- Running mass column update.
-- Running: yb_mass_column_update
/* dryrun, this query will update 31944 row/s */ UPDATE "{db1}"."dev"."data_types_t" SET "col8" = REPLACE(col8, 'a', '') WHERE INSTR(col8, 'a') > 0
/* dryrun, this query will update 2540 row/s */ UPDATE "{db1}"."dev"."data_types_t" SET "col9" = REPLACE(col9, 'a', '') WHERE INSTR(col9, 'a') > 0
-- 2 column/s need to be updated
-- Completed mass column update."""
        , stderr='')

    , test_case(
        cmd=(
            """yb_mass_column_update.py @{argsdir}/db1 --datatype_like 'CHAR%%' """
            """--update_where_clause "INSTR(<columnname>, 'a') > 0" """
            """--set_clause "REPLACE(<columnname>, 'a', '')" """
            """--pre_sql 'BEGIN WORK;' --post_sql 'ROLLBACK WORK;' --exec_updates""")
        , exit_code=0
        , stdout="""-- Running mass column update.
-- Running: yb_mass_column_update
/* updating 31944 row/s */ UPDATE "{db1}"."dev"."data_types_t" SET "col8" = REPLACE(col8, 'a', '') WHERE INSTR(col8, 'a') > 0
/* updating 2540 row/s */ UPDATE "{db1}"."dev"."data_types_t" SET "col9" = REPLACE(col9, 'a', '') WHERE INSTR(col9, 'a') > 0
-- 2 column/s have been updated
-- Completed mass column update."""
        , stderr='')
]