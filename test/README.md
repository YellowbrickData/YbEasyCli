# Testing

## Running Tests

Navigate to the `test` directory to run any test. Current tests require a
pre-defined set of database objects.

&nbsp;&nbsp;&nbsp;&nbsp;```step1_create_db_objects.sh``` -- creates all the
database objects required to run

&nbsp;&nbsp;&nbsp;&nbsp;```step3_drop_db_objects.sh``` -- cleans up all database
objects by dropping

To personalize the tests, customize the user and database names.

&nbsp;&nbsp;&nbsp;&nbsp;```settings.py``` -- modify this file to set
custom user and databases used for running tests

### Running All Tests
To run all tests, use the given shell script

&nbsp;&nbsp;&nbsp;&nbsp;```./step2_run_all_tests.sh <host>```

### Running Individual Tests
For the full set of test run options run

&nbsp;&nbsp;&nbsp;&nbsp;e.g. ```./test_run.py --help```

To run individual tests, pass the module name you would like to test as a
command line argument to the test script

&nbsp;&nbsp;&nbsp;&nbsp;e.g. ```./test_run.py yb_get_table_names```

### Reading Results
Each test will present a line of output in the following form

&nbsp;&nbsp;&nbsp;&nbsp;```Test: <Passed/Failed>, To Run: <test command>```

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
