#!/usr/bin/env python3
"""
USAGE:
      yb_ddl_sequence.py [options]

PURPOSE:
      Dump out the SQL/DDL that was used to create a sequence.

OPTIONS:
      See the command line help message for all options.
      (yb_ddl_sequence.py --help)

Output:
      SQL DDL (the CREATE SEQUENCE statements).
"""

import yb_ddl_object

yb_ddl_object.main('sequence')
