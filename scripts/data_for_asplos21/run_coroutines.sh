#!/bin/bash
declare -A hybrid_code
hybrid_code["bank","nowait"]=28
hybrid_code["bank","waitdie"]=28
hybrid_code["bank","mvcc"]=28
hybrid_code["bank","sundial"]=20
hybrid_code["bank","occ"]=62

hybrid_code["ycsb","nowait"]=31
hybrid_code["ycsb","waitdie"]=31
hybrid_code["ycsb","mvcc"]=31
hybrid_code["ycsb","sundial"]=31
hybrid_code["ycsb","occ"]=63

hybrid_code["tpcc","nowait"]=28
hybrid_code["tpcc","waitdie"]=28
hybrid_code["tpcc","mvcc"]=28
hybrid_code["tpcc","sundial"]=31
hybrid_code["tpcc","occ"]=30


for app in bank ycsb; do
#for app in ycsb; do
    cp co2.$app co2
    #for co in `seq 1 2 20`; do
    for co in `seq 1 2 12`; do
        outpath=out/eval_coroutines/cor$co
        mkdir -p $outpath
        #for alg in nowait waitdie occ mvcc sundial; do
        for alg in occ; do
            #for version in rpc onesided; do
            for version in rpc onesided hybrid; do
                if [ $version == "hybrid" ]; then
                    echo "compiling $alg $version...with hybrid_code=${hybrid_code[$app,$alg]}"
                    ( cd ../../newbuild/ && ./make_hybrid.sh $alg ${hybrid_code[$app,$alg]} ) &
                    wait
                    sleep 5
                    echo "running $alg $version...with hybrid_code=${hybrid_code[$app,$alg]}"
                else
                    echo "running $alg $version..."
                fi
                ./y.sh $alg $version 0 $app $co $outpath &
                process_id=$!
                ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 $app $co $outpath"  &
                ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 $app $co $outpath"  &
                ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 $app $co $outpath"  &
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
done
