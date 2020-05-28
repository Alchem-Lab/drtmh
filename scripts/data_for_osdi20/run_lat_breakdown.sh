#!/bin/bash

cp co2.bank co2
outpath=out/lat-breakdown/
mkdir -p $outpath
for alg in occ sundial mvcc nowait waitdie; do
    for version in rpc onesided; do
        echo "running $alg $version..."
        ./y.sh $alg $version 0 bank 1 $outpath &
        ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 1 bank 1 $outpath"  &
        ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 2 bank 1 $outpath"  &
        ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./y.sh $alg $version 3 bank 1 $outpath"  &
        wait
    done
done
