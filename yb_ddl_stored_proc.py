#!/usr/bin/env python3
"""
USAGE:
      yb_ddl_stored_proc.py [options]

PURPOSE:
      Dump out the SQL/DDL that was used to create a stored procedure.

OPTIONS:
      See the command line help message for all options.
      (yb_ddl_stored_proc.py --help)

Output:
      SQL DDL (the CREATE STORED PROCEDURE statements).
"""

import yb_ddl_object

yb_ddl_object.main('ddl_stored_proc')
