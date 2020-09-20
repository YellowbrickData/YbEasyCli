#!/usr/bin/env python3
"""
USAGE:
      yb_ddl_view.py [options]

PURPOSE:
      Dump out the SQL/DDL that was used to create a view.

OPTIONS:
      See the command line help message for all options.
      (yb_ddl_view.py --help)

Output:
      SQL DDL (the CREATE VIEW statements).
"""

import yb_ddl_object

yb_ddl_object.main('view')
