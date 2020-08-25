#!/bin/bash

for app in ycsb bank; do
#for app in bank; do
    cp co2.$app co2
    for co in `seq 13 2 20`; do
    #for co in 17; do
        outpath=out/eval_coroutines_calvin/cor$co
        mkdir -p $outpath
        #for alg in nowait waitdie occ mvcc sundial; do
        for alg in calvin; do
            #for version in rpc onesided; do
            for version in rpc onesided; do
                echo "running $alg $version..."
                ./y.sh $alg $version 0 $app $co $outpath &
                process_id=$!
                ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 $app $co $outpath"  &
                ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 $app $co $outpath"  &
                ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 $app $co $outpath"  &
                wait $process_id
                ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                wait
                echo "$alg-$version on $app ends."
                sleep 10
            done
        done
    done
done
