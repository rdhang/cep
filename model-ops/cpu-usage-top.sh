#!/bin/bash
#PNAME="$1"
LOG_FILE="$1"
#PID=$(pidof ${PNAME})

# Original
#top -b -d 1 -p $PID | awk \
#    -v cpuLog="$LOG_FILE" -v pid="$PID" -v pname="$PNAME" '
#    /^top -/{time = $3}
#    $1+0>0 {printf "%s %s :: %s[%s] CPU Usage: %d%%\n", \
#            strftime("%Y-%m-%d"), time, pname, pid, $9 > cpuLog
#            fflush(cpuLog)}'

top -b -d 1 | awk \
    -v cpuLog=out-`date +%Y-%m-%d_%H-%M-%S`.csv '
    /^top -/{time = $3}
    $1+0>0 {printf "%s %s,%d,%s,%d,%d,%d,%d,%d,%s,%.1f,%.1f,%s\n", \
            strftime("%Y-%m-%d"), time, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $12 |& "/inet/tcp/9999/0/0"}'
	#	close("/inet/tcp/9999/0/0")}'


