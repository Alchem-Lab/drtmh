#!/bin/bash


for version in hybrid; do
    #for app in bank ycsb tpcc; do
    for app in tpcc; do
        cp co2.$app co2
            
        alg="occ"
        #for hybrid_code in 32 0 48 16 40 8 56 24 36 4 52 20 44 12 60 28 34 2 50 18 42 10 58 26 38 6 54 22 46 14 62 30 33 1 49 17 41 9 57 25 37 5 53 21 45 13 61 29 35 3 51 19 43 11 59 27 39 7 55 23 47 15 63 31;do
        for hybrid_code in 33 54;do
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

