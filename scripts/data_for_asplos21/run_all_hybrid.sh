#!/bin/bash

#for app in tpcc bank ycsb; do
for app in bank; do
    cp co2.$app co2
#    for alg in occ sundial mvcc nowait waitdie; do
    for alg in occ; do
        for version in hybrid; do
            # for hc in `seq 0 63`; do
            for hc in 0; do
                echo "compiling $alg $version...with hybrid_code=$hc"
                ( cd ../../newbuild/ && ./make_hybrid.sh $alg $hc ) &
                wait

                echo "running $alg $version...with hybrid_code=$hc"
                outpath=out/hybrid/$alg/$hc
                mkdir -p $outpath

                ./y.sh $alg $version 0 $app 10 $outpath &
                process_id=$!
                ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 $app 10 $outpath"  &
                ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 $app 10 $outpath"  &
                ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 $app 10 $outpath"  &
                wait $process_id
                ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && pkill -e nocc$alg-$version --signal SIGINT"  &
                wait
                echo "$alg-$version on $app with hybrid code $hc ends."
                sleep 5
            done
        done
    done
done

