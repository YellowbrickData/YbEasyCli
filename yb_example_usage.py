class example_usage:
    conn_args_file = {'$HOME/conn.args': """--host yb14
--dbuser dze
--conn_db stores"""}

    examples = {
        'yb_analyze_columns': {
            'cmd_line_args': '@$HOME/conn.args --schema_in dev --table sales --column_in store_id price --'
            , 'file_args': [conn_args_file] }
        , 'yb_check_db_views': {
            'cmd_line_args': '@$HOME/conn.args --database_in stores'
            , 'file_args': [conn_args_file] }
        , 'yb_chunk_dml_by_date_part': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_date_part.args --print_chunk_dml'
            , 'file_args': [ conn_args_file
                , {'$HOME/yb_chunk_dml_by_date_part.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_ts\"\"\"
--column 'sale_ts'
--date_part HOUR
--chunk_rows 100000000"""} ] }
        , 'yb_chunk_dml_by_integer': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_date_part.args --print_chunk_dml'
            , 'file_args': [ conn_args_file
                , {'$HOME/yb_chunk_dml_by_date_part.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_id\"\"\"
--column 'sale_id'
--chunk_rows 100000000"""} ] }
        , 'yb_chunk_dml_by_yyyymmdd_integer': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/yb_chunk_dml_by_date_part.args --print_chunk_dml'
            , 'file_args': [ conn_args_file
                , {'$HOME/yb_chunk_dml_by_date_part.args': """--table dze_db1.dev.sales
--dml \"\"\"INSERT INTO sales_chunk_ordered
SELECT *
FROM dze_db1.dev.sales
WHERE <chunk_where_clause>
ORDER BY sale_date_int\"\"\"
--column 'sale_date_int'
--chunk_rows 100000000"""} ] }
        , 'yb_chunk_optimal_rows': {
            'cmd_line_args': '@$HOME/conn.args --table dze_db1.dev.sales --schema dev'
            , 'file_args': [conn_args_file] }
        , 'yb_ddl_sequence': {
            'cmd_line_args': "@$HOME/conn.args --current_schema dev --sequence_like '%id%' --"
            , 'file_args': [conn_args_file] }
        , 'yb_ddl_table': {
            'cmd_line_args': "@$HOME/conn.args --current_schema dev  --table_like 'sale_%' --"
            , 'file_args': [conn_args_file] }
        , 'yb_ddl_view': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --with_db --view_like '%sale%' --"
            , 'file_args': [conn_args_file] }
        , 'yb_find_columns': {
            'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' 'TIME%' --"
            , 'file_args': [conn_args_file] }
        , 'yb_get_column_name': {
            'cmd_line_args': "@$HOME/conn.args --schema dev --object sales --column price --"
            , 'file_args': [conn_args_file] }
        , 'yb_get_column_names': {
            'cmd_line_args': "@$HOME/conn.args --schema dev -- sales"
            , 'file_args': [conn_args_file] }
        , 'yb_get_column_type': {
            'cmd_line_args': "@$HOME/conn.args --schema dev --table sales --column price --"
            , 'file_args': [conn_args_file] }
        , 'yb_get_sequence_names': {
            'cmd_line_args': "@$HOME/conn.args --schema_in dev Prod --sequence_like '%price%' --sequence_NOTlike '%id%' --"
            , 'file_args': [conn_args_file] }
        , 'yb_get_table_distribution_key': {
            'cmd_line_args': "@$HOME/conn.args --schema Prod --table sales --"
            , 'file_args': [conn_args_file] }
        , 'yb_get_table_name': {
            'cmd_line_args': '@$HOME/conn.args --current_schema dev --table sales --'
            , 'file_args': [conn_args_file] }
        , 'yb_get_table_names': {
            'cmd_line_args': '--host yb14 --dbuser dze --conn_db stores --schema_in dev --'}
        , 'yb_get_view_name': {
            'cmd_line_args': '@$HOME/conn.args --schema Prod --view sales_v --'
            , 'file_args': [conn_args_file] }
        , 'yb_get_view_names': {
            'cmd_line_args': '@$HOME/conn.args --schema_in dev Prod --'
            , 'file_args': [conn_args_file] }
        , 'yb_is_cstore_table': {
            'cmd_line_args': '@$HOME/conn.args --table sys.blade --'
            , 'file_args': [conn_args_file] }
        , 'yb_mass_column_update': {
            'cmd_line_args': "@$HOME/conn.args --datatype_like 'CHAR%' --update_where_clause \"<columnname> = 'NULL'\" --set_clause NULL --"
            , 'file_args': [conn_args_file] }
        , 'yb_rstore_query_to_cstore_table': {
            'cmd_line_args': '@$HOME/conn.args @$HOME/sys_schema.args --'
            , 'file_args': [conn_args_file
            	, { '$HOME/sys_schema.args': """--query \"\"\"
SELECT name
FROM sys.schema
\"\"\"
--table 'sys_schema'"""} ] }
        , 'yb_to_yb_copy_table': {
            'cmd_line_args': "@$HOME/conn.args --unload_where_clause 'sale_id BETWEEN 1000 AND 2000' --src_table Prod.sales --dst_table dev.sales --log_dir tmp --"
            , 'file_args': [ {'$HOME/conn.args': """--src_host yb14
--src_dbuser dze
--src_conn_db stores_prod
--dst_host yb89
--dst_dbuser dze
--dst_conn_db stores_dev"""} ] } }