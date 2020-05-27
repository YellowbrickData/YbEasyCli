# Testing

## Running Tests

Navigate to the `test` directory to run any test. Current tests require a
pre-defined set of database objects.

&nbsp;&nbsp;&nbsp;&nbsp;```test_create_db_objects.sh``` -- creates all the
database objects required to run

&nbsp;&nbsp;&nbsp;&nbsp;```test_drop_db_objects.sh``` -- cleans up all database
objects by dropping

To personalize the tests, customize the user and database names.

&nbsp;&nbsp;&nbsp;&nbsp;```test_constants.py``` -- modify this file to set
custom user and databases used for running tests

<sub>Note: `test_create_db_objects.sh` creates a new database user. There may be a
lag of ~30 seconds between when the user create statement is run and when the
user actually becomes available.</sub> 

### Running All Tests
To run all tests, use the given shell script

&nbsp;&nbsp;&nbsp;&nbsp;```./test_run_all_utils.sh <host>```

### Running Individual Tests
To run individual tests, pass the module name you would like to test as a
command line argument to the test script

&nbsp;&nbsp;&nbsp;&nbsp;e.g. ```./test_run.py -h <host> yb_get_table_names```

### Reading Results
Each test will present a line of output in the following form

&nbsp;&nbsp;&nbsp;&nbsp;```Test: <Passed/Failed>, Type: <type of test>, Command:
<command executed>```

The test only shows the output if the test has failed. To see the output of a
passed test, copy just the command and execute it by itself. For more
information, adjust the verbosity with the `--verbose` flag.


## Developing Tests

To test a newly developed utility script, create a file with a name that mirrors
that of the script being tested and a prefix of `test_cases__`. For example, the
script `yb_get_table_names.py` has an accompanying file
`test_cases__yb_get_table_names.py`. 

This file should contain a list of `test_case` instances. Please review the code
in [test_run.py](./test_run.py) as well as other `test_cases__` for examples.
