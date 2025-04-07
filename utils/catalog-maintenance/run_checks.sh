#!/bin/sh
LOGFILE=run_checks_$(date +%Y%m%d_%H%M%S).out.log
date > $LOGFILE
ybsql -Xq -f catalog_pre_checks.sql | tee -a $LOGFILE
date >> $LOGFILE
