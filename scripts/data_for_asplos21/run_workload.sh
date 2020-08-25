#!/bin/bash

cp co2.ycsb co2.ycsb.bak

#for time in 1 2 4 8 16 32 64 128 256; do
for time in 32; do
    sed -i "s?<sleep_time>\(.*\)</sleep_time>?<sleep_time>$time</sleep_time>?g" co2.ycsb
    cp co2.ycsb co2
    outpath=out/sleep_curve/sleep$time
    mkdir -p $outpath
    #for alg in nowait waitdie occ mvcc sundial; do
    for alg in occ; do
        #for version in rpc onesided; do
        for version in onesided; do
            echo "running $alg $version..."
            ./y.sh $alg $version 0 ycsb 10 $outpath &
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 ycsb 10 $outpath"  &
            ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 ycsb 10 $outpath"  &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 ycsb 10 $outpath"  &
            wait
            sleep 3
        done
    done
done

cp co2.ycsb.bak co2.ycsb
