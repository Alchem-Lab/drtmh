#!/bin/bash

START_MAC=5
END_MAC=16

if [ "$1x" != "x" ]; then
	START_MAC=$1
fi

if [ "$2x" != "x" ]; then
	END_MAC=$2
fi

for i in `seq ${START_MAC} ${END_MAC}`; do
	echo -n "mac$i "
	salloc -N 1 -t 00:00:05 --nodelist=nerv$i ssh nerv$i /home/$USER/git_repos/drtmh/scripts/cat_hugepage.sh
done
