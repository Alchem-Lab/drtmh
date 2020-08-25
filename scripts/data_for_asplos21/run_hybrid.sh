#!/bin/bash


for version in hybrid; do
    #for app in tpcc bank ycsb; do
    for app in tpcc; do
        cp co2.$app co2
        #for alg in occ sundial mvcc nowait waitdie; do
        for alg in occ; do
        #for alg in nowait waitdie mvcc sundial; do
            hybrid_code=0
            if [ $alg == "nowait" ]; then
                #When the index fetching cost can not be ignored (i.e., with the use of rdma_read_val instead of rdma_lookup_op):
                #hybrid_code=0      #rpc case
                #hybrid_code=31      #onesided case
                #hybrid_code=4      #onesided Log; RPC Lock/Release/Commit (the best)
                #hybrid_code=28     #onesided Log; (RPC Lock + one-sided Release/commit) (inefficient case)
            
                #With index cache locally (we can simply cherry pick according to the latency breakdown):
                #hybrid_code=28     #onesided Log; (RPC Lock + one-sided Release/commit)
            
                #cherry-pick for ycsb
                #hybrid_code=31
                
                #cherry-pick for tpcc
                hybrid_code=28
            fi

            if [ $alg == "waitdie" ]; then
                #When the index fetching cost can not be ignored (i.e., with the use of rdma_read_val instead of rdma_lookup_op):
                #hybrid_code=0      #rpc case
                #hybrid_code=31      #onesided case
                #hybrid_code=4      #onesided Log; RPC Lock/Release/Commit (the best)
                #hybrid_code=28     #onesided Log; (RPC Lock + one-sided Release/commit) (inefficient case)
            
                #With index cache locally (we can simply cherry pick according to the latency breakdown):
                #hybrid_code=28     #onesided Log; (RPC Lock + one-sided Release/commit)
            
                #cherry-pick for ycsb
                #hybrid_code=31

                #cherry-pick for tpcc
                hybrid_code=28
            fi
            if [ $alg == "mvcc" ]; then

                #When the index fetching cost can not be ignored (i.e., with the use of rdma_read_val instead of rdma_lookup_op):
                #hybrid_code=0      #rpc case
                #hybrid_code=31     #onesided case
                #hybrid_code=4      #onesided Log; RPC Read; RPC Lock/Release/Commit (the best)
                #hybrid_code=28     #onesided Log; RPC Read; (RPC Lock + one-sided Release/commit) (inefficient case)
                #hybrid_code=12     #onesided Log; RPC Read; (RPC Lock + one-sided Release + RPC commit) (inefficient case)
                #hybrid_code=29      #onesided Log; RPC Read; one-sided Lock/Release/Commit; 
                
                #With index cache locally (we can simply cherry pick according to the latency breakdown):
                #hybrid_code=28     #onesided Log; RPC Read; (RPC Lock + one-sided Release/commit)
            
                #cherry-pick for ycsb
                #hybrid_code=31

                #cherry-pick for tpcc
                hybrid_code=28
            fi

            if [ $alg == 'sundial' ]; then
                #When the index fetching cost can not be ignored (i.e., with the use of rdma_read_val instead of rdma_lookup_op):
                #hybrid_code=0      #rpc case
                #hybrid_code=127    #onesided case
                #hybrid_code=4      #onesided Log; RPC Read/Renew; RPC Lock/Release/Commit    (the best)
                #hybrid_code=29     #onesided Log; RPC Read/Renew; one-sided Lock/Release/Commit; 
                #hybrid_code=28      #onesided Log; RPC Read/Renew; (RPC Lock + one-sided Release/Commit)

                #With index cache locally (we can simply cherry pick according to the latency breakdown):
                #hybrid_code=20      #onesided log/commit; RPC read/lock/renew;
            
                #cherry-pick for ycsb
                #hybrid_code=31

                #cherry-pick for tpcc
                hybrid_code=31
            fi

            if [ $alg == 'occ' ]; then
                #With index cache locally (we can simply cherry pick according to the latency breakdown):
                #hybrid_code=0      #rpc case
                #hybrid_code=63    #onesided case
                #hybrid_code=62      #RPC lock; onesided for all others;
            
                #cherry-pick for ycsb
                #hybrid_code=63     #onesided for all stages.

                #cherry-pick for tpcc
                hybrid_code=30     #RPC lock and validate; onesided for all others.
            fi

            echo "compiling $alg $version...with hybrid_code=$hybrid_code"
            ( cd ../../newbuild/ && ./make_hybrid.sh $alg $hybrid_code ) &
            wait

            sleep 5

            cor=1
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

