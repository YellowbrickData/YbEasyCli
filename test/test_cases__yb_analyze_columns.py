test_cases = [
    test_case(cmd='yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_in col3 --output_format 3 --level 2'
        , exit_code=0
        , stdout="""-- Running column analysis.
ANALYSIS OF: {db1}.dev.data_types_t.col3
-------------------------------------------------
Column is: NULLABLE
Column Position Ordinal: 3
Data Type              : SMALLINT
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 10001
Min Value              : 1
Max Value              : 10001
Is Unique              : FALSE
-- Completed column analysis."""
        , stderr=''
        , map_out={r'-{10,300}' : ''})
 
    , test_case(cmd="yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_like 'col1%' --output_format 1 --level 1"
        , exit_code=0
        , stdout="""-- Running column analysis.
database    column                    table  data                         is          bytes
                                      order  type                         1null         max
                                                                          2dist
                                                                          3sort
                                                                          4clust
                                                                          5part
----------  ----------------------  -------  ---------------------------  --------  -------
{db1}     dev.data_types_t.col1         1  BIGINT                       XX---           8
{db1}     dev.data_types_t.col10       10  DATE                         X----           4
{db1}     dev.data_types_t.col11       11  TIME WITHOUT TIME ZONE       X----           8
{db1}     dev.data_types_t.col12       12  TIMESTAMP WITHOUT TIME ZONE  X----           8
{db1}     dev.data_types_t.col13       13  TIMESTAMP WITH TIME ZONE     X----           8
{db1}     dev.data_types_t.col14       14  IPV4                         X----           4
{db1}     dev.data_types_t.col15       15  IPV6                         X----          16
{db1}     dev.data_types_t.col16       16  MACADDR                      X----           8
{db1}     dev.data_types_t.col17       17  MACADDR8                     X----           8
{db1}     dev.data_types_t.col18       18  BOOLEAN                      X----           1
{db1}     dev.data_types_t.col19       19  INTEGER                      X----           4
-- Completed column analysis."""
        , stderr='')
  
    , test_case(cmd="yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_like 'col1%' --output_format 2 --level 2"
        , exit_code=0
        , stdout="""-- Running column analysis.
database|column|table_order|data_type|is_1null_2dist_3sort_4clust_5part|bytes_max|count_rows|count_nulls|count_distinct|char_bytes_min|char_bytes_max|char_bytes_avg|char_bytes_total|max_len_int|max_len_frac|is_uniq
{db1}|dev.data_types_t.col1|1|BIGINT|XX---|8|1000000|0|1000000|||||||X
{db1}|dev.data_types_t.col10|10|DATE|X----|4|1000000|0|2410|||||||-
{db1}|dev.data_types_t.col11|11|TIME WITHOUT TIME ZONE|X----|8|1000000|0|2419|||||||-
{db1}|dev.data_types_t.col12|12|TIMESTAMP WITHOUT TIME ZONE|X----|8|1000000|0|109343|||||||-
{db1}|dev.data_types_t.col13|13|TIMESTAMP WITH TIME ZONE|X----|8|1000000|0|109342|||||||-
{db1}|dev.data_types_t.col14|14|IPV4|X----|4|1000000|0|462574|||||||-
{db1}|dev.data_types_t.col15|15|IPV6|X----|16|1000000|0|462574|||||||-
{db1}|dev.data_types_t.col16|16|MACADDR|X----|8|1000000|0|462574|||||||-
{db1}|dev.data_types_t.col17|17|MACADDR8|X----|8|1000000|0|462574|||||||-
{db1}|dev.data_types_t.col18|18|BOOLEAN|X----|1|1000000|0|2|||||||-
{db1}|dev.data_types_t.col19|19|INTEGER|X----|4|1000000|0|2410|||||||-
-- Completed column analysis."""
        , stderr='')
 
    , test_case(cmd='yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_in col3 --output_format 3 --level 3'
        , exit_code=0
        , stdout="""-- Running column analysis.
ANALYSIS OF: {db1}.dev.data_types_t.col3
-------------------------------------------------
Column is: NULLABLE
Column Position Ordinal: 3
Data Type              : SMALLINT
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 10001
Min Value              : 1
Max Value              : 10001
Is Unique              : FALSE
Group: 1,     Row Count: 100, % of Total Rows:   0.0100, Value: 10
Group: 2,     Row Count: 100, % of Total Rows:   0.0100, Value: 100
Group: 3,     Row Count: 100, % of Total Rows:   0.0100, Value: 1000
Group: 4,     Row Count: 100, % of Total Rows:   0.0100, Value: 10000
Group: 5,     Row Count: 100, % of Total Rows:   0.0100, Value: 1001
Group: 6,     Row Count: 100, % of Total Rows:   0.0100, Value: 1002
Group: 7,     Row Count: 100, % of Total Rows:   0.0100, Value: 1003
Group: 8,     Row Count: 100, % of Total Rows:   0.0100, Value: 1004
Group: 9,     Row Count: 100, % of Total Rows:   0.0100, Value: 9990
Group: 10,    Row Count: 100, % of Total Rows:   0.0100, Value: 9991
...
Group: 9992,  Row Count: 100, % of Total Rows:   0.0100, Value: 9992
Group: 9993,  Row Count: 100, % of Total Rows:   0.0100, Value: 9993
Group: 9994,  Row Count: 100, % of Total Rows:   0.0100, Value: 9994
Group: 9995,  Row Count: 100, % of Total Rows:   0.0100, Value: 9995
Group: 9996,  Row Count: 100, % of Total Rows:   0.0100, Value: 9996
Group: 9997,  Row Count: 100, % of Total Rows:   0.0100, Value: 9997
Group: 9998,  Row Count: 100, % of Total Rows:   0.0100, Value: 9998
Group: 9999,  Row Count: 100, % of Total Rows:   0.0100, Value: 9999
Group: 10000, Row Count: 99,  % of Total Rows:   0.0099, Value: 1
Group: 10001, Row Count: 1,   % of Total Rows:   0.0001, Value: 10001
-- Completed column analysis."""
        , stderr=''
        , map_out={r'-{10,300}' : ''})
 
    , test_case(cmd='yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_in col8 --output_format 3 --level 3'
        , exit_code=0
        , stdout="""-- Running column analysis.
ANALYSIS OF: {db1}.dev.data_types_t.col8
------------------------------------------
Column is: NULLABLE
Column Position Ordinal: 8
Data Type              : CHARACTER VARYING(256)
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : !!!!
Max Value              : {{{{u)
Min Length             : 4
Max Length             : 4
Average Length         : 4
Total Character Bytes  : 4000000
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 9!!!
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: :!!R
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 8!!!
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 9!!1
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 9!!Y
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!\\
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!m
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!p
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 9!!d
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 9!!x
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!#
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!(
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!)
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!+
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!6
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!7
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!9
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!:
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!=
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!A
-- Completed column analysis."""
		, stderr=''
		, map_out={r'-{10,300}' : ''})

    , test_case(cmd='yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --column_in col13  --output_format 3 --level 3'
        , exit_code=0
        , stdout="""-- Running column analysis.
ANALYSIS OF: {db1}.dev.data_types_t.col13
------------------------------------------------------
Column is: NULLABLE
Column Position Ordinal: 13
Data Type              : TIMESTAMP WITH TIME ZONE
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 109342
Min Value              : 2020-01-01 03:00:01-08
Max Value              : 2021-12-02 17:48:01-08
Is Unique              : FALSE
Group: 1,      Row Count: 324618, % of Total Rows:  32.4618, Value: 2020-01-01 03:00:01-08
Group: 2,      Row Count: 200,    % of Total Rows:   0.0200, Value: 2020-01-08 03:00:01-08
Group: 3,      Row Count: 198,    % of Total Rows:   0.0198, Value: 2020-01-04 15:00:01-08
Group: 4,      Row Count: 186,    % of Total Rows:   0.0186, Value: 2020-01-11 15:00:01-08
Group: 5,      Row Count: 184,    % of Total Rows:   0.0184, Value: 2020-01-08 19:48:01-08
Group: 6,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-04 23:24:01-08
Group: 7,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-05 07:48:01-08
Group: 8,      Row Count: 180,    % of Total Rows:   0.0180, Value: 2020-01-06 06:12:01-08
Group: 9,      Row Count: 176,    % of Total Rows:   0.0176, Value: 2020-01-07 13:00:01-08
Group: 10,     Row Count: 170,    % of Total Rows:   0.0170, Value: 2020-01-02 12:36:01-08
...
Group: 109333, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:00:47-08
Group: 109334, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:00:51-08
Group: 109335, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:01:04-08
Group: 109336, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:01:17-08
Group: 109337, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:01:25-08
Group: 109338, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:01:55-08
Group: 109339, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:01:58-08
Group: 109340, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:02:21-08
Group: 109341, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:02:25-08
Group: 109342, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 03:02:31-08
-- Completed column analysis."""
		, stderr=''
		, map_out={r'-{10,300}' : '', r'\d{2}:\d{2}:\d{2}(\-|\+)\d{2}' : 'HH:MM:SS-TZ'})

    , test_case(cmd='yb_analyze_columns.py @{argsdir}/db1 --table data_types_t --schema_in dev --level 3'
        , exit_code=0
        , stdout="""-- Running column analysis.
ANALYSIS OF: {db1}.dev.data_types_t.col1

Column is: NULLABLE, DISTRIBUTION KEY
Column Position Ordinal: 1
Data Type              : BIGINT
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 1000000
Min Value              : 1
Max Value              : 1000000
Is Unique              : TRUE


ANALYSIS OF: {db1}.dev.data_types_t.col2

Column is: NULLABLE
Column Position Ordinal: 2
Data Type              : INTEGER
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 1000000
Min Value              : 1
Max Value              : 1000000
Is Unique              : TRUE


ANALYSIS OF: {db1}.dev.data_types_t.col3

Column is: NULLABLE
Column Position Ordinal: 3
Data Type              : SMALLINT
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 10001
Min Value              : 1
Max Value              : 10001
Is Unique              : FALSE
Group: 1,     Row Count: 100, % of Total Rows:   0.0100, Value: 10
Group: 2,     Row Count: 100, % of Total Rows:   0.0100, Value: 100
Group: 3,     Row Count: 100, % of Total Rows:   0.0100, Value: 1000
Group: 4,     Row Count: 100, % of Total Rows:   0.0100, Value: 10000
Group: 5,     Row Count: 100, % of Total Rows:   0.0100, Value: 1001
Group: 6,     Row Count: 100, % of Total Rows:   0.0100, Value: 1002
Group: 7,     Row Count: 100, % of Total Rows:   0.0100, Value: 1003
Group: 8,     Row Count: 100, % of Total Rows:   0.0100, Value: 1004
Group: 9,     Row Count: 100, % of Total Rows:   0.0100, Value: 9990
Group: 10,    Row Count: 100, % of Total Rows:   0.0100, Value: 9991
...
Group: 9992,  Row Count: 100, % of Total Rows:   0.0100, Value: 9992
Group: 9993,  Row Count: 100, % of Total Rows:   0.0100, Value: 9993
Group: 9994,  Row Count: 100, % of Total Rows:   0.0100, Value: 9994
Group: 9995,  Row Count: 100, % of Total Rows:   0.0100, Value: 9995
Group: 9996,  Row Count: 100, % of Total Rows:   0.0100, Value: 9996
Group: 9997,  Row Count: 100, % of Total Rows:   0.0100, Value: 9997
Group: 9998,  Row Count: 100, % of Total Rows:   0.0100, Value: 9998
Group: 9999,  Row Count: 100, % of Total Rows:   0.0100, Value: 9999
Group: 10000, Row Count: 99,  % of Total Rows:   0.0099, Value: 1
Group: 10001, Row Count: 1,   % of Total Rows:   0.0001, Value: 10001


ANALYSIS OF: {db1}.dev.data_types_t.col4

Column is: NULLABLE
Column Position Ordinal: 4
Data Type              : NUMERIC(18,0)
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 500000
Min Value              : 1000000
Max Value              : 250000500000
Max Digits Integer     : 12
Is Unique              : FALSE
Group: 1,      Row Count: 2, % of Total Rows:   0.0002, Value: 1000000
Group: 2,      Row Count: 2, % of Total Rows:   0.0002, Value: 100000371898
Group: 3,      Row Count: 2, % of Total Rows:   0.0002, Value: 100001146494
Group: 4,      Row Count: 2, % of Total Rows:   0.0002, Value: 100001921088
Group: 5,      Row Count: 2, % of Total Rows:   0.0002, Value: 100002695680
Group: 6,      Row Count: 2, % of Total Rows:   0.0002, Value: 100003470270
Group: 7,      Row Count: 2, % of Total Rows:   0.0002, Value: 100004244858
Group: 8,      Row Count: 2, % of Total Rows:   0.0002, Value: 100005019444
Group: 9,      Row Count: 2, % of Total Rows:   0.0002, Value: 100005794028
Group: 10,     Row Count: 2, % of Total Rows:   0.0002, Value: 100006568610
...
Group: 499991, Row Count: 2, % of Total Rows:   0.0002, Value: 99994949670
Group: 499992, Row Count: 2, % of Total Rows:   0.0002, Value: 99995724280
Group: 499993, Row Count: 2, % of Total Rows:   0.0002, Value: 99996498888
Group: 499994, Row Count: 2, % of Total Rows:   0.0002, Value: 99997273494
Group: 499995, Row Count: 2, % of Total Rows:   0.0002, Value: 99998048098
Group: 499996, Row Count: 2, % of Total Rows:   0.0002, Value: 99998822700
Group: 499997, Row Count: 2, % of Total Rows:   0.0002, Value: 9999910
Group: 499998, Row Count: 2, % of Total Rows:   0.0002, Value: 9999959698
Group: 499999, Row Count: 2, % of Total Rows:   0.0002, Value: 99999597300
Group: 500000, Row Count: 2, % of Total Rows:   0.0002, Value: 999999000


ANALYSIS OF: {db1}.dev.data_types_t.col5

Column is: NULLABLE
Column Position Ordinal: 5
Data Type              : REAL
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 979339
Min Value              : 99.99
Max Value              : 9.98003e+07
Is Unique              : FALSE
Group: 1,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00201e+07
Group: 2,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00301e+07
Group: 3,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00401e+07
Group: 4,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00501e+07
Group: 5,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00601e+07
Group: 6,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00701e+07
Group: 7,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00801e+07
Group: 8,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.00901e+07
Group: 9,      Row Count: 100, % of Total Rows:   0.0100, Value: 5.01001e+07
Group: 10,     Row Count: 100, % of Total Rows:   0.0100, Value: 5.01101e+07
...
Group: 979330, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00002e+07
Group: 979331, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00003e+07
Group: 979332, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00004e+07
Group: 979333, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00006e+07
Group: 979334, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00007e+07
Group: 979335, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00008e+07
Group: 979336, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00009e+07
Group: 979337, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00011e+07
Group: 979338, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00012e+07
Group: 979339, Row Count: 1,   % of Total Rows:   0.0001, Value: 1.00014e+07


ANALYSIS OF: {db1}.dev.data_types_t.col6

Column is: NULLABLE
Column Position Ordinal: 6
Data Type              : DOUBLE PRECISION
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 999999
Min Value              : 99.99000099
Max Value              : 99800299.8
Is Unique              : FALSE
Group: 1,      Row Count: 2, % of Total Rows:   0.0002, Value: 49990101.9796041
Group: 2,      Row Count: 1, % of Total Rows:   0.0001, Value: 1000000
Group: 3,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000188.8886667
Group: 4,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000277.7771111
Group: 5,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000366.6653333
Group: 6,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000455.5533333
Group: 7,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000544.4411111
Group: 8,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000633.3286667
Group: 9,      Row Count: 1, % of Total Rows:   0.0001, Value: 10000722.216
Group: 10,     Row Count: 1, % of Total Rows:   0.0001, Value: 10000811.1031111
...
Group: 999990, Row Count: 1, % of Total Rows:   0.0001, Value: 10000899.99
Group: 999991, Row Count: 1, % of Total Rows:   0.0001, Value: 9998722.36351516
Group: 999992, Row Count: 1, % of Total Rows:   0.0001, Value: 9998811.242973
Group: 999993, Row Count: 1, % of Total Rows:   0.0001, Value: 9998900.12220864
Group: 999994, Row Count: 1, % of Total Rows:   0.0001, Value: 9998989.00122208
Group: 999995, Row Count: 1, % of Total Rows:   0.0001, Value: 9999.01
Group: 999996, Row Count: 1, % of Total Rows:   0.0001, Value: 99990.09108197
Group: 999997, Row Count: 1, % of Total Rows:   0.0001, Value: 999900.01009998
Group: 999998, Row Count: 1, % of Total Rows:   0.0001, Value: 9999910
Group: 999999, Row Count: 1, % of Total Rows:   0.0001, Value: 999998.990001


ANALYSIS OF: {db1}.dev.data_types_t.col7

Column is: NULLABLE
Column Position Ordinal: 7
Data Type              : UUID
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 1000000
Is Unique              : TRUE


ANALYSIS OF: {db1}.dev.data_types_t.col8

Column is: NULLABLE
Column Position Ordinal: 8
Data Type              : CHARACTER VARYING(256)
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : !!!!
Max Value              : {{{{u)
Min Length             : 4
Max Length             : 4
Average Length         : 4
Total Character Bytes  : 4000000
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 9!!!
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: :!!R
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 8!!!
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 9!!1
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 9!!Y
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!\\
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!m
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 9!!p
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 9!!d
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 9!!x
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!#
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!(
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!)
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!+
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!6
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!7
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!9
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!:
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!=
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: !!!A


ANALYSIS OF: {db1}.dev.data_types_t.col9

Column is: NULLABLE
Column Position Ordinal: 9
Data Type              : CHARACTER(1)
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 82
Min Value              : "
Max Value              : |
Min Length             : 1
Max Length             : 1
Average Length         : 1
Total Character Bytes  : 1000000
Is Unique              : FALSE
Group: 1,  Row Count: 200920, % of Total Rows:  20.0920, Value: :
Group: 2,  Row Count: 85166,  % of Total Rows:   8.5166, Value: 9
Group: 3,  Row Count: 65888,  % of Total Rows:   6.5888, Value: 8
Group: 4,  Row Count: 55902,  % of Total Rows:   5.5902, Value: 7
Group: 5,  Row Count: 49524,  % of Total Rows:   4.9524, Value: 6
Group: 6,  Row Count: 44992,  % of Total Rows:   4.4992, Value: 5
Group: 7,  Row Count: 41552,  % of Total Rows:   4.1552, Value: 4
Group: 8,  Row Count: 38834,  % of Total Rows:   3.8834, Value: 3
Group: 9,  Row Count: 36606,  % of Total Rows:   3.6606, Value: 2
Group: 10, Row Count: 34744,  % of Total Rows:   3.4744, Value: 1
...
Group: 73, Row Count: 2382,   % of Total Rows:   0.2382, Value: E
Group: 74, Row Count: 2380,   % of Total Rows:   0.2380, Value: D
Group: 75, Row Count: 2372,   % of Total Rows:   0.2372, Value: C
Group: 76, Row Count: 2366,   % of Total Rows:   0.2366, Value: B
Group: 77, Row Count: 2364,   % of Total Rows:   0.2364, Value: A
Group: 78, Row Count: 2356,   % of Total Rows:   0.2356, Value: ?
Group: 79, Row Count: 2356,   % of Total Rows:   0.2356, Value: @
Group: 80, Row Count: 2348,   % of Total Rows:   0.2348, Value: >
Group: 81, Row Count: 2342,   % of Total Rows:   0.2342, Value: =
Group: 82, Row Count: 2340,   % of Total Rows:   0.2340, Value: <


ANALYSIS OF: {db1}.dev.data_types_t.col10

Column is: NULLABLE
Column Position Ordinal: 10
Data Type              : DATE
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 2410
Min Value              : 2020-01-01
Max Value              : 2042-03-06
Is Unique              : FALSE
Group: 1,    Row Count: 166582, % of Total Rows:  16.6582, Value: 2020-01-01
Group: 2,    Row Count: 3812,   % of Total Rows:   0.3812, Value: 2022-04-20
Group: 3,    Row Count: 3660,   % of Total Rows:   0.3660, Value: 2025-01-22
Group: 4,    Row Count: 3654,   % of Total Rows:   0.3654, Value: 2021-12-21
Group: 5,    Row Count: 3542,   % of Total Rows:   0.3542, Value: 2025-11-30
Group: 6,    Row Count: 3532,   % of Total Rows:   0.3532, Value: 2022-10-05
Group: 7,    Row Count: 3242,   % of Total Rows:   0.3242, Value: 2023-08-13
Group: 8,    Row Count: 3228,   % of Total Rows:   0.3228, Value: 2024-08-07
Group: 9,    Row Count: 3176,   % of Total Rows:   0.3176, Value: 2022-12-16
Group: 10,   Row Count: 3168,   % of Total Rows:   0.3168, Value: 2023-12-11
...
Group: 2401, Row Count: 24,     % of Total Rows:   0.0024, Value: 2020-01-26
Group: 2402, Row Count: 24,     % of Total Rows:   0.0024, Value: 2020-01-28
Group: 2403, Row Count: 24,     % of Total Rows:   0.0024, Value: 2020-02-05
Group: 2404, Row Count: 24,     % of Total Rows:   0.0024, Value: 2020-02-07
Group: 2405, Row Count: 24,     % of Total Rows:   0.0024, Value: 2020-02-11
Group: 2406, Row Count: 22,     % of Total Rows:   0.0022, Value: 2020-01-30
Group: 2407, Row Count: 22,     % of Total Rows:   0.0022, Value: 2020-02-01
Group: 2408, Row Count: 22,     % of Total Rows:   0.0022, Value: 2020-05-05
Group: 2409, Row Count: 22,     % of Total Rows:   0.0022, Value: 2020-06-04
Group: 2410, Row Count: 22,     % of Total Rows:   0.0022, Value: 2020-07-22


ANALYSIS OF: {db1}.dev.data_types_t.col11

Column is: NULLABLE
Column Position Ordinal: 11
Data Type              : TIME WITHOUT TIME ZONE
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 2419
Min Value              : 01:01:01
Max Value              : 03:16:01
Is Unique              : FALSE
Group: 1,    Row Count: 229770, % of Total Rows:  22.9770, Value: 01:01:01
Group: 2,    Row Count: 1686,   % of Total Rows:   0.1686, Value: 01:13:01
Group: 3,    Row Count: 1664,   % of Total Rows:   0.1664, Value: 01:07:01
Group: 4,    Row Count: 1516,   % of Total Rows:   0.1516, Value: 01:15:01
Group: 5,    Row Count: 1506,   % of Total Rows:   0.1506, Value: 01:22:01
Group: 6,    Row Count: 1470,   % of Total Rows:   0.1470, Value: 01:05:01
Group: 7,    Row Count: 1462,   % of Total Rows:   0.1462, Value: 01:04:01
Group: 8,    Row Count: 1440,   % of Total Rows:   0.1440, Value: 01:09:25
Group: 9,    Row Count: 1426,   % of Total Rows:   0.1426, Value: 01:08:01
Group: 10,   Row Count: 1372,   % of Total Rows:   0.1372, Value: 01:43:01
...
Group: 2410, Row Count: 90,     % of Total Rows:   0.0090, Value: 01:11:26
Group: 2411, Row Count: 90,     % of Total Rows:   0.0090, Value: 01:15:02
Group: 2412, Row Count: 88,     % of Total Rows:   0.0088, Value: 01:29:02
Group: 2413, Row Count: 88,     % of Total Rows:   0.0088, Value: 02:22:41
Group: 2414, Row Count: 86,     % of Total Rows:   0.0086, Value: 01:26:22
Group: 2415, Row Count: 84,     % of Total Rows:   0.0084, Value: 01:17:02
Group: 2416, Row Count: 84,     % of Total Rows:   0.0084, Value: 01:42:41
Group: 2417, Row Count: 82,     % of Total Rows:   0.0082, Value: 01:09:50
Group: 2418, Row Count: 80,     % of Total Rows:   0.0080, Value: 01:03:02
Group: 2419, Row Count: 80,     % of Total Rows:   0.0080, Value: 01:44:22


ANALYSIS OF: {db1}.dev.data_types_t.col12

Column is: NULLABLE
Column Position Ordinal: 12
Data Type              : TIMESTAMP WITHOUT TIME ZONE
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 109343
Min Value              : 2020-01-01 00:00:01
Max Value              : 2021-12-02 14:48:01
Is Unique              : FALSE
Group: 1,      Row Count: 324618, % of Total Rows:  32.4618, Value: 2020-01-01 00:00:01
Group: 2,      Row Count: 200,    % of Total Rows:   0.0200, Value: 2020-01-08 00:00:01
Group: 3,      Row Count: 198,    % of Total Rows:   0.0198, Value: 2020-01-04 12:00:01
Group: 4,      Row Count: 186,    % of Total Rows:   0.0186, Value: 2020-01-11 12:00:01
Group: 5,      Row Count: 184,    % of Total Rows:   0.0184, Value: 2020-01-08 16:48:01
Group: 6,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-04 20:24:01
Group: 7,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-05 04:48:01
Group: 8,      Row Count: 180,    % of Total Rows:   0.0180, Value: 2020-01-06 03:12:01
Group: 9,      Row Count: 176,    % of Total Rows:   0.0176, Value: 2020-01-07 10:00:01
Group: 10,     Row Count: 170,    % of Total Rows:   0.0170, Value: 2020-01-02 09:36:01
...
Group: 109334, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:00:47
Group: 109335, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:00:51
Group: 109336, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:01:04
Group: 109337, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:01:17
Group: 109338, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:01:25
Group: 109339, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:01:55
Group: 109340, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:01:58
Group: 109341, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:02:21
Group: 109342, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:02:25
Group: 109343, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 00:02:31


ANALYSIS OF: {db1}.dev.data_types_t.col13

Column is: NULLABLE
Column Position Ordinal: 13
Data Type              : TIMESTAMP WITH TIME ZONE
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 109342
Min Value              : 2020-01-01 HH:MM:SS-TZ
Max Value              : 2021-12-02 HH:MM:SS-TZ
Is Unique              : FALSE
Group: 1,      Row Count: 324618, % of Total Rows:  32.4618, Value: 2020-01-01 HH:MM:SS-TZ
Group: 2,      Row Count: 200,    % of Total Rows:   0.0200, Value: 2020-01-08 HH:MM:SS-TZ
Group: 3,      Row Count: 198,    % of Total Rows:   0.0198, Value: 2020-01-04 HH:MM:SS-TZ
Group: 4,      Row Count: 186,    % of Total Rows:   0.0186, Value: 2020-01-11 HH:MM:SS-TZ
Group: 5,      Row Count: 184,    % of Total Rows:   0.0184, Value: 2020-01-08 HH:MM:SS-TZ
Group: 6,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-04 HH:MM:SS-TZ
Group: 7,      Row Count: 182,    % of Total Rows:   0.0182, Value: 2020-01-05 HH:MM:SS-TZ
Group: 8,      Row Count: 180,    % of Total Rows:   0.0180, Value: 2020-01-06 HH:MM:SS-TZ
Group: 9,      Row Count: 176,    % of Total Rows:   0.0176, Value: 2020-01-07 HH:MM:SS-TZ
Group: 10,     Row Count: 170,    % of Total Rows:   0.0170, Value: 2020-01-02 HH:MM:SS-TZ
...
Group: 109333, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109334, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109335, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109336, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109337, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109338, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109339, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109340, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109341, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ
Group: 109342, Row Count: 2,      % of Total Rows:   0.0002, Value: 2020-01-01 HH:MM:SS-TZ


ANALYSIS OF: {db1}.dev.data_types_t.col14

Column is: NULLABLE
Column Position Ordinal: 14
Data Type              : IPV4
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : 0.0.0.0
Max Value              : 90.90.84.8
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 24.0.0.0
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: 25.0.0.49
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 23.0.0.0
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 24.0.0.16
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 24.0.0.56
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 24.0.0.59
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 24.0.0.76
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 24.0.0.79
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 24.0.0.7
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 24.0.0.87
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.10
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.2
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.21
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.22
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.24
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.25
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.28
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.32
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.34
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: 0.0.0.46


ANALYSIS OF: {db1}.dev.data_types_t.col15

Column is: NULLABLE
Column Position Ordinal: 15
Data Type              : IPV6
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : 0000:0000:0000:0000:0000:0000:0000:0000
Max Value              : 0090:0090:0084:0008:0090:0090:0084:0008
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 0024:0000:0000:0000:0024:0000:0000:0000
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: 0025:0000:0000:0049:0025:0000:0000:0049
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 0023:0000:0000:0000:0023:0000:0000:0000
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 0024:0000:0000:0016:0024:0000:0000:0016
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 0024:0000:0000:0056:0024:0000:0000:0056
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 0024:0000:0000:0059:0024:0000:0000:0059
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 0024:0000:0000:0076:0024:0000:0000:0076
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 0024:0000:0000:0079:0024:0000:0000:0079
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 0024:0000:0000:0067:0024:0000:0000:0067
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 0024:0000:0000:0087:0024:0000:0000:0087
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0002:0000:0000:0000:0002
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0007:0000:0000:0000:0007
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0008:0000:0000:0000:0008
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0010:0000:0000:0000:0010
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0021:0000:0000:0000:0021
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0022:0000:0000:0000:0022
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0024:0000:0000:0000:0024
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0025:0000:0000:0000:0025
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0028:0000:0000:0000:0028
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: 0000:0000:0000:0032:0000:0000:0000:0032


ANALYSIS OF: {db1}.dev.data_types_t.col16

Column is: NULLABLE
Column Position Ordinal: 16
Data Type              : MACADDR
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : 00:00:00:00:00:00
Max Value              : 90:90:84:08:90:90
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 24:00:00:00:24:00
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: 25:00:00:49:25:00
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 23:00:00:00:23:00
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 24:00:00:16:24:00
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 24:00:00:56:24:00
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:59:24:00
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:76:24:00
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:79:24:00
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 24:00:00:67:24:00
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 24:00:00:87:24:00
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:02:00:00
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:07:00:00
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:08:00:00
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:10:00:00
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:21:00:00
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:22:00:00
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:24:00:00
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:25:00:00
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:28:00:00
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:32:00:00


ANALYSIS OF: {db1}.dev.data_types_t.col17

Column is: NULLABLE
Column Position Ordinal: 17
Data Type              : MACADDR8
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 462574
Min Value              : 00:00:00:00:00:00:00:00
Max Value              : 90:90:84:08:90:90:84:08
Is Unique              : FALSE
Group: 1,      Row Count: 984, % of Total Rows:   0.0984, Value: 24:00:00:00:24:00:00:00
Group: 2,      Row Count: 198, % of Total Rows:   0.0198, Value: 25:00:00:49:25:00:00:49
Group: 3,      Row Count: 122, % of Total Rows:   0.0122, Value: 23:00:00:00:23:00:00:00
Group: 4,      Row Count: 108, % of Total Rows:   0.0108, Value: 24:00:00:16:24:00:00:16
Group: 5,      Row Count: 108, % of Total Rows:   0.0108, Value: 24:00:00:56:24:00:00:56
Group: 6,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:59:24:00:00:59
Group: 7,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:76:24:00:00:76
Group: 8,      Row Count: 104, % of Total Rows:   0.0104, Value: 24:00:00:79:24:00:00:79
Group: 9,      Row Count: 102, % of Total Rows:   0.0102, Value: 24:00:00:67:24:00:00:67
Group: 10,     Row Count: 102, % of Total Rows:   0.0102, Value: 24:00:00:87:24:00:00:87
...
Group: 462565, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:02:00:00:00:02
Group: 462566, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:07:00:00:00:07
Group: 462567, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:08:00:00:00:08
Group: 462568, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:10:00:00:00:10
Group: 462569, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:21:00:00:00:21
Group: 462570, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:22:00:00:00:22
Group: 462571, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:24:00:00:00:24
Group: 462572, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:25:00:00:00:25
Group: 462573, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:28:00:00:00:28
Group: 462574, Row Count: 2,   % of Total Rows:   0.0002, Value: 00:00:00:32:00:00:00:32


ANALYSIS OF: {db1}.dev.data_types_t.col18

Column is: NULLABLE
Column Position Ordinal: 18
Data Type              : BOOLEAN
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 2
Is Unique              : FALSE


ANALYSIS OF: {db1}.dev.data_types_t.col19

Column is: NULLABLE
Column Position Ordinal: 19
Data Type              : INTEGER
Row Count              : 1000000
Row Count with NULLS   : 0
Row Distinct Count     : 2410
Min Value              : 20200101
Max Value              : 20420306
Is Unique              : FALSE
Group: 1,    Row Count: 166582, % of Total Rows:  16.6582, Value: 20200101
Group: 2,    Row Count: 3812,   % of Total Rows:   0.3812, Value: 20220420
Group: 3,    Row Count: 3660,   % of Total Rows:   0.3660, Value: 20250122
Group: 4,    Row Count: 3654,   % of Total Rows:   0.3654, Value: 20211221
Group: 5,    Row Count: 3542,   % of Total Rows:   0.3542, Value: 20251130
Group: 6,    Row Count: 3532,   % of Total Rows:   0.3532, Value: 20221005
Group: 7,    Row Count: 3242,   % of Total Rows:   0.3242, Value: 20230813
Group: 8,    Row Count: 3228,   % of Total Rows:   0.3228, Value: 20240807
Group: 9,    Row Count: 3176,   % of Total Rows:   0.3176, Value: 20221216
Group: 10,   Row Count: 3168,   % of Total Rows:   0.3168, Value: 20231211
...
Group: 2401, Row Count: 24,     % of Total Rows:   0.0024, Value: 20200126
Group: 2402, Row Count: 24,     % of Total Rows:   0.0024, Value: 20200128
Group: 2403, Row Count: 24,     % of Total Rows:   0.0024, Value: 20200205
Group: 2404, Row Count: 24,     % of Total Rows:   0.0024, Value: 20200207
Group: 2405, Row Count: 24,     % of Total Rows:   0.0024, Value: 20200211
Group: 2406, Row Count: 22,     % of Total Rows:   0.0022, Value: 20200130
Group: 2407, Row Count: 22,     % of Total Rows:   0.0022, Value: 20200201
Group: 2408, Row Count: 22,     % of Total Rows:   0.0022, Value: 20200505
Group: 2409, Row Count: 22,     % of Total Rows:   0.0022, Value: 20200604
Group: 2410, Row Count: 22,     % of Total Rows:   0.0022, Value: 20200722
-- Completed column analysis."""
		, stderr=''
		, map_out={r'-{10,300}' : '', r'\d{2}:\d{2}:\d{2}(\-|\+)\d{2}' : 'HH:MM:SS-TZ'})
]