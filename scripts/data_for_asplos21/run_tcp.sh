#!/bin/bash

outpath=out/tcp_comp_rpc/
mkdir -p $outpath
for app in ycsb; do
    cp co2.$app co2
    #for alg in occ sundial mvcc nowait waitdie; do
    for alg in mvcc nowait waitdie; do
        for version in tcp rpc; do
            echo "running $alg $version..."
            ./y.sh $alg $version 0 $app 10 $outpath &
            process_id=$!
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 $app 10 $outpath"  &
            ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 $app 10 $outpath"  &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 $app 10 $outpath"  &
            wait $process_id
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
            ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
            wait
            echo "$alg-$version on $app ends."
            sleep 30
        done
    done
done

