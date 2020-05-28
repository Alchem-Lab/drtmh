#!/bin/bash

idx=$1
if [ $idx == "" ]; then
    exit
fi

for hap in 50 60 70 80 90 95 99; do
    ./run1.sh $idx 10 out/contention/hotrate_$hap/ 
done
