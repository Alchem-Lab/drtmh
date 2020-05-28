#!/bin/bash
#for qp in 1; do
#for qp in 30 40 50 60 70 80 90 100; do
for qp in 1; do
    outpath=out/scalability/qp_$qp
    mkdir -p $outpath

    #echo "compiling algs with qps=$qp..."
    #(cd ../../newbuild && ./a.sh $qp) &
    #wait

    #for alg in occ sundial mvcc nowait waitdie; do
    for alg in waitdie; do
        #for version in rpc onesided; do
        for version in rpc; do
            port=$(( 8800 + RANDOM % 100 ))
            sed -i 's?<port>.*</port>?<port>'$port'</port>?g' config.xml
            echo "running $alg $version at port $port with qps=$qp..."
            ./run_qp.sh $alg $version 0 ycsb 10 $outpath  &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./run_qp.sh $alg $version 1 ycsb 10 $outpath"  &
            ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./run_qp.sh $alg $version 2 ycsb 10 $outpath"  &
            ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_osdi20/ && ./run_qp.sh $alg $version 3 ycsb 10 $outpath"  &
            wait
        done
    done
done
