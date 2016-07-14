# swift DR : alternative swift container sync module

## USAGE

### 1) copy example.py to filename_you_want.py

### 2) fill the src container and dest container information

### 3) python filename_you_want.py to run container sync



## FIXME

### 1) Have not tested syncing SLO

### 2) It first creates / updates containers / files and then deletes containers / files, so a storage capacity problem may arise. But it's not the package bug. If you want to avoid such problems, just change the order of creation / updating and deleting sequence.



## PACKAGE DEPENDENCIES

### iso8601