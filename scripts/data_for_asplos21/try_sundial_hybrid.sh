#!/bin/bash


for version in hybrid; do
    #for app in tpcc bank ycsb; do
    for app in bank; do
        cp co2.$app co2
            
        alg="sundial"
        #for hybrid_code in 64 0 80 16 72 8 88 24 68 4 84 20 76 12 92 28 66 2 82 18 74 10 90 26 70 6 86 22 78 14 94 30 65 1 81 17 73 9 89 25 69 5 85 21 77 13 93 29 67 3 83 19 75 11 91 27 71 7 87 23 79 15 95 31; do
        for hybrid_code in 0; do
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

