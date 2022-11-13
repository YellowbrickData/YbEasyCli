# lab

This directory serves as a laboratory for utilities that are functional but are not properly documented or still in a crud form.

## Scripts

| Script                       | Description                                                                                                                       |
|:-----------------------------|:----------------------------------------------------------------------------------------------------------------------------------|
| gucs.sh                      | Saves GUCs in a file, could be useful when doing upgrades (save before and after then compare to see if something got lost/reset) |
| pgcat-fs-mapping.sh          | Shows mapping between catalog tables and corresponding entries on the file system                                                 |
| selective-backup.py          | Does smart backups by checking first if there was any data change since the last successful backup                                |
| ssl-trust.py                 | Manages SSL trust (required for replication) between two appliances (source and target)                                           |
| summarize_lime_status_log.py | Displays a summary of critical cluster events (failovers, database restarts, blade crashes, etc)                                  |
