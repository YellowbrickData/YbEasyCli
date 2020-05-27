#!/usr/bin/env python3
"""
USAGE:
      yb_ddl_table.py [options]

PURPOSE:
      Dump out the SQL/DDL that was used to create a table.

OPTIONS:
      See the command line help message for all options.
      (yb_ddl_table.py --help)

Output:
      SQL DDL (the CREATE TABLE statements).
"""

import yb_ddl_object

yb_ddl_object.ddl_object('table')
