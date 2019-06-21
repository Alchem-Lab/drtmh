#!/bin/bash

HOST_CNT=2
if [ x$1 != "x" ];then
	HOST_CNT=$1
fi

IFCONFIG_PATH="../ifconfig.txt"
if [ -f ${IFCONFIG_PATH} ]; then
  rm -fr ${IFCONFIG_PATH}
fi

TIME_ALLOC='00:2:00'
if [ x$3 != 'x' ]; then
    	echo $3 | sed 's/,/\n/g' >> ${IFCONFIG_PATH}
   	salloc -N $1 -t $TIME_ALLOC --nodelist=$3 ./run.sh $2
else
	LAST_HOST=16
	FIRST_HOST=`echo $LAST_HOST - $HOST_CNT + 1 | bc`
	HOSTS_CSV=`seq -s, -f"nerv%.0f" $FIRST_HOST 1 $LAST_HOST`
	HOSTS=`seq -f"nerv%.0f" $FIRST_HOST 1 $LAST_HOST`
	for HOSTNAME in ${HOSTS}; do
	  echo ${HOSTNAME}  >> ${IFCONFIG_PATH}
	done
    	
	salloc -N $1 -t $TIME_ALLOC --nodelist=$HOSTS_CSV ./run.sh $2
fi
