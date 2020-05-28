#!/bin/bash

outpath=out/overall/edr/
mkdir -p $outpath
#for app in bank ycsb tpcc; do
for app in bank; do
    cp co2.$app co2
    #for alg in occ sundial mvcc nowait waitdie; do
    for alg in nowait; do
        for version in onesided; do
            echo "running $alg $version..."
            ./y.sh $alg $version 0 $app 10 $outpath &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 1 $app 10 $outpath"  &
            ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 2 $app 10 $outpath"  &
            ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 3 $app 10 $outpath"  &
            wait
        done
    done
done

