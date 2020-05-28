#!/bin/bash

protocol=$1
version=$2
id=$3
app=$4
cornum=$5
outpath=$6
echo "protocol=$protocol version=$version id=$id app=$app cornum=$cornum outpath=$outpath"

cat config.xml | grep "port"
cat config.xml | grep "tx_hot"
cat config.xml | grep "num_hot"
if [ $app == "ycsb" ]; then
    app_=bank
else
    app_=$app
fi

date
../nocc$protocol-$version --bench $app_ --txn-flags 1  --verbose --config config.xml --id $id -t 10 -c $cornum -r 100 -p 4  1>$outpath/drtmh-nocc$protocol-$app-4-$version.log_$id 2>&1
