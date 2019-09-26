Tests were run on a EPYC 7401p server with 128GB RAM and a SAMSUNG MZQLB1T9HAJR-00007 NVMe running CentOS 7. Data partition used was formatted as ext4 (journaling disabled). Server process was limited to one cpu (via docker's --cpuset-cpus option). Host networking was used on server's docker container. Clients were bound to a specific cpu running on the host via pthread_setaffinity_np(). CLOCK_MONOTONIC was used for measurements.

System info/modified params:

[root@xx ~]# uname -a
Linux xx 5.2.11-1.el7.elrepo.x86_64 #1 SMP Thu Aug 29 08:10:52 EDT 2019 x86_64 x86_64 x86_64 GNU/Linux

[root@xx ~]# cat /proc/cmdline
BOOT_IMAGE=/vmlinuz-5.2.11-1.el7.elrepo.x86_64 root=xx ro biosdevname=0 crashkernel=auto nomodeset rd.auto=1 consoleblank=0 mem=5G

[root@xx ~]# cat /proc/sys/vm/zone_reclaim_mode
0

[root@xx ~]# cat /sys/block/nvme0n1/queue/read_ahead_kb
0

[root@xx hrvoje]# cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]

[root@xx ~]# cat /data01/mongod.conf
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log

storage:
  dbPath: /data/mongodb
  journal:
    enabled: true
  wiredTiger:
    engineConfig:
       cacheSizeGB: 2.6

processManagement:
  fork: false
  pidFilePath: /var/run/mongodb/mongod.pid
  timeZoneInfo: /usr/share/zoneinfo

net:
  port: 27017
  bindIp: 127.0.0.1

Dependencies:
 fmt - https://github.com/fmtlib/fmt
 tdigest - https://github.com/ajwerner/tdigestc
 mongo-c-driver - https://github.com/mongodb/mongo-c-driver

Compiled with:
[root@devel ~]# clang++ -o mongo.o -c -std=c++17 -fPIC -Wall -O3 -fomit-frame-pointer -I. -I/usr/include/libbson-1.0 -I/usr/include/libmongoc-1.0 mongo.cpp