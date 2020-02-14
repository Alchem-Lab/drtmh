#!/bin/bash

mac_id=$1
alg=$2
rpc_or_onesided=$3
qp_num=$4

./nocc${alg}-${rpc_or_onesided} --bench bank --txn-flags 1  --verbose --config config.xml --id ${mac_id} -t 8 -c 10 -r 100 -p 4 2>&1 | tee qp_out/drtmh-nocc${alg}-bank-4-${rpc_or_onesided}-qp${qp_num}.log_${mac_id}
