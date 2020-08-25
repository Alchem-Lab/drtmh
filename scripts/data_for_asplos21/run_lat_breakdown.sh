#!/bin/bash

app=tpcc
cp co2.$app co2
#for alg in occ sundial mvcc nowait waitdie; do
for alg in nowait waitdie occ mvcc sundial; do
    for version in rpc onesided; do
        echo "running $alg $version..."
        cor=10
        outpath=out/lat-breakdown/cor-$cor/
        mkdir -p $outpath

        ./y.sh $alg $version 0 $app $cor $outpath &
        process_id=$!
        ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 $app $cor $outpath"  &
        ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 $app $cor $outpath"  &
        ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 $app $cor $outpath"  &
        wait $process_id
        ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
        ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
        ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
        wait
        echo "$alg-$version on $app ends."
        sleep 3
    done
done
