for co in `seq 1 2 20`; do for f in `ls cor$co`; do mv cor$co/$f `echo cor$co/$f | sed 's/\(.*\)ycsb\(.*\)/\1bank\2/g'`; done; done
