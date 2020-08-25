#!/bin/bash


for version in hybrid; do
    #for app in tpcc bank ycsb; do
    #for app in bank ycsb tpcc; do
    for app in tpcc; do
        cp co2.$app co2
            
        alg="mvcc"
        #for hybrid_code in 16 0 24 8 20 4 28 12 18 2 26 10 22 6 30 14 17 1 25 9 21 5 29 13 19 3 27 11 23 7 31 15; do
        for hybrid_code in 27 11 23 7 31 15; do
            echo "compiling $alg $version...with hybrid_code=$hybrid_code"
            ( cd ../../newbuild/ && ./make_hybrid.sh $alg $hybrid_code ) &
            wait
            sleep 5

            cor=10
            echo "running $alg $version...with hybrid_code=$hybrid_code"
            outpath="out/hybrid/cor_$cor/$app/$alg/hc_$hybrid_code/"
            mkdir -p $outpath

            echo "running $alg $version..."
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
            sleep 5
        done
    done
done

