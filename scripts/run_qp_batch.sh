#!/bin/bash

#for qps in `seq 1 10`; do
#for qps in `seq 99 -10 19`; do
for qps in 4; do
    #for alg in 'occ' 'nowait' 'waitdie' 'mvcc' 'sundial'; do
    for alg in 'sundial'; do
        echo "compiling $alg qps=$qps..."
        (cd ../newbuild && ./make_sundial.sh $alg $qps) &
        wait
        #for mode in 'rpc' 'onesided'; do
        for mode in 'onesided'; do
            port=$(( 8800 + RANDOM % 100 ))
            sed -i 's?<port>.*</port>?<port>'$port'</port>?g' config.xml
            echo "running $alg qps=$qps $mode at port $port..."
            ssh gorgon5 'export LD_LIBRARY_PATH=/home/huangkz/usr/lib/:$LD_LIBRARY_PATH && cd usc/new/drtmh-calvim/scripts/ && bash ./run_qp.sh 0'" $alg $mode $qps" &
            ssh gorgon6 'export LD_LIBRARY_PATH=/home/huangkz/usr/lib/:$LD_LIBRARY_PATH && cd usc/new/drtmh-calvim/scripts/ && bash ./run_qp.sh 1'" $alg $mode $qps" &
            ssh gorgon7 'export LD_LIBRARY_PATH=/home/huangkz/usr/lib/:$LD_LIBRARY_PATH && cd usc/new/drtmh-calvim/scripts/ && bash ./run_qp.sh 2'" $alg $mode $qps" &
            ssh gorgon8 'export LD_LIBRARY_PATH=/home/huangkz/usr/lib/:$LD_LIBRARY_PATH && cd usc/new/drtmh-calvim/scripts/ && bash ./run_qp.sh 3'" $alg $mode $qps" &
            wait
        done
    done
done
