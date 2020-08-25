#!/bin/bash
#for qp in 1; do
#for qp in 30 40 50 60 70 80 90 100; do
cp co2.ycsb co2
for qp in 20; do
#for qp in 1 10 20 30 40; do
    outpath=out/scalability/qp_$qp
    mkdir -p $outpath

    echo "compiling algs with qps=$qp..."
    (cd ../../newbuild && ./a.sh $qp) &
    wait

    #for alg in occ sundial mvcc nowait waitdie; do
    for alg in mvcc; do
        #for version in rpc onesided; do
        for version in onesided; do

	        cat co1 > config.xml
            port=$(( 8800 + RANDOM % 100 ))
	        echo "  <port>$port</port>" >> config.xml 
	        cat co2 >> config.xml
            # sed -i 's?<port>.*</port>?<port>'$port'</port>?g' config.xml
            echo "running $alg $version at port $port with qps=$qp..."
            ./run_qp.sh $alg $version 0 ycsb 10 $outpath  &
            ssh gorgon4 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./run_qp.sh $alg $version 1 ycsb 10 $outpath"  &
            ssh gorgon5 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./run_qp.sh $alg $version 2 ycsb 10 $outpath"  &
            ssh gorgon6 "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH && cd usc/new/drtmh/scripts/data_for_asplos21/ && ./run_qp.sh $alg $version 3 ycsb 10 $outpath"  &
            wait
            sleep 3
        done
    done
done

echo "ALL DONE. Defaults to original qp=1 before scalability test; compiling algs with qps=1..."
(cd ../../newbuild && ./a.sh) &
wait

