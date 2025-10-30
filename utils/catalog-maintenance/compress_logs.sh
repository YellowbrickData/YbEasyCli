#!/bin/bash
TARGZ=catalog-maintenance-logs-$(date +%Y%m%d-%H%M%S).tgz
tar zcvf $TARGZ *.out.???
rm -f *.out.???
echo All logs are compressed into $TARGZ