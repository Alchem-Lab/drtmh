#!/bin/bash

cp co2.ycsb co2.ycsb.bak

for hap in 50 60 70 80 90 95 99; do
    sed -i "s?<tx_hot>\(.*\)</tx_hot>?<tx_hot>$hap</tx_hot>?g" co2.ycsb
    grep 'tx_hot' co2.ycsb
    ./run0.sh 10 out/contention/hotrate_$hap/ 
done

cp co2.ycsb.bak co2.ycsb
