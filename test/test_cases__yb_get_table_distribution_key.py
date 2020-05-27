test_cases = [
    test_case(
        cmd='yb_get_table_distribution_key.py -h %s -U %s -D %s --verbose 3' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=2,
        stdout='',
        stderr=(
        "usage: yb_get_table_distribution_key.py [database] table [options]\n"
            "yb_get_table_distribution_key.py: error: the following arguments "
            "are required: table")),

    test_case(
        cmd=
        'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev a1_t'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=
        'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev x1_t'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev '
            'dist_random_t') % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='RANDOM',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev '
            'dist_replicate_t') % (self.host,
                                   self.test_user_name,
                                   self.test_db1),
        exit_code=0,
        stdout='REPLICATED',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev '
            '%s a1_t') % (self.host,
                          self.test_user_name,
                          self.test_db1,
                          self.test_db2),
        exit_code=0,
        stdout='',
        stderr='',
        comment=(
            'Use of sys.tables cross database does not work, so no value is '
            'returned. The table can be accessed but is returning no rows\nThe '
            'sys.vt_table_info has the same issue and also is only accessible '
            'by super users.')
    ),

    test_case(
        cmd=
        'yb_get_table_distribution_key.py -h %s -U %s -D %s --schema dev %s a1_t'
        % (self.host, self.test_user_name, self.test_db2, self.test_db2),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_distribution_key.py -h %s -U %s -D %s --owner %s '
            '--schema dev %s a1_t') % (self.host,
                                       self.test_user_name,
                                       self.test_db2,
                                       self.test_user_name,
                                       self.test_db2),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_distribution_key.py -h %s -U %s -D %s --owner %s '
            '--schema dev %s a1_t') % (self.host,
                                       self.test_user_name,
                                       self.test_db2,
                                       'no_such_owner',
                                       self.test_db2),
        exit_code=0,
        stdout='',
        stderr='')
]
