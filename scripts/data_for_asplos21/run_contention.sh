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


app=ycsb
cp co2.ycsb co2.ycsb.bak

#for hap in 60; do
#for hap in 99 95 90 80 70 60 50 25 0; do
for hap in 99 95 90 80 70 50 25 0; do
    sed -i "s?<tx_hot>\(.*\)</tx_hot>?<tx_hot>`expr 100 - $hap`</tx_hot>?g" co2.ycsb
    cp co2.ycsb co2
    outpath=out/contention/hotrate_$hap
    mkdir -p $outpath
    #for alg in occ; do
    for alg in nowait waitdie occ mvcc sundial; do
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

            co=1
            ./y.sh $alg $version 0 ycsb $co $outpath &
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 1 ycsb $co $outpath"  &
            ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 2 ycsb $co $outpath"  &
            ssh gorgon2 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./y.sh $alg $version 3 ycsb $co $outpath"  &
            wait
            sleep 3
        done
    done
done

cp co2.ycsb.bak co2.ycsb
