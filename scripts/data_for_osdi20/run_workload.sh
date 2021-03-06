#!/bin/bash

cp co2.ycsb co2.ycsb.bak

for time in 4; do
    sed -i "s?<sleep_time>\(.*\)</sleep_time>?<sleep_time>$time</sleep_time>?g" co2.ycsb
    cp co2.ycsb co2
    outpath=out/sleep_curve/sleep$time
    mkdir -p $outpath
    for alg in waitdie; do
        for version in rpc; do
            echo "running $alg $version..."
            ./y.sh $alg $version 0 ycsb 10 $outpath &
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 1 ycsb 10 $outpath"  &
            ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 2 ycsb 10 $outpath"  &
            ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 3 ycsb 10 $outpath"  &
            wait
        done
    done
done

cp co2.ycsb.bak co2.ycsb
