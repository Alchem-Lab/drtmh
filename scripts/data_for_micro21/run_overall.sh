#!/bin/bash

outpath=out/overall/edr/ycsb_high_contention/
mkdir -p $outpath
#for app in tpcc bank ycsb; do
for app in ycsb; do
    cp co2.$app co2
#    for alg in occ sundial mvcc nowait waitdie; do
    for alg in mvcc; do
        for version in tcp rpc onesided; do
            echo "running $alg $version..."

            for cor in 1; do
                ./y.sh $alg $version 0 $app $cor $outpath &
                process_id=$!
                ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_micro21/ && touch ./y.sh && ./y.sh $alg $version 1 $app $cor $outpath"  &
                ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_micro21/ && touch ./y.sh && ./y.sh $alg $version 2 $app $cor $outpath"  &
                ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_micro21/ && touch ./y.sh && ./y.sh $alg $version 3 $app $cor $outpath"  &
                wait $process_id
                ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon7 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon8 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                wait
                echo "$alg-$version on $app ends."
                sleep 5
            done
        done
    done
done

