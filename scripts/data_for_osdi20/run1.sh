cornum=$2
outpath=$3

mkdir -p $outpath
sleep_time=10

# bank
#sleep $sleep_time
#./y.sh occ  rpc $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh occ  onesided $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh sundial  rpc $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh sundial  onesided $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh mvcc  rpc $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh mvcc  onesided $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh nowait  rpc $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh nowait  onesided $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh waitdie  rpc $1 bank $cornum $outpath
#sleep $sleep_time
#./y.sh waitdie  onesided $1 bank $cornum $outpath
##sleep $sleep_time
##./y.sh calvin  rpc $1 bank $cornum $outpath
##sleep $sleep_time
##./y.sh calvin  onesided $1 bank $cornum $outpath
##sleep $sleep_time


# ycsb
sleep $sleep_time
./y.sh occ  rpc $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh occ  onesided $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh sundial  rpc $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh sundial  onesided $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh mvcc  rpc $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh mvcc  onesided $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh nowait  rpc $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh nowait  onesided $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh waitdie  rpc $1 ycsb $cornum $outpath
sleep $sleep_time
./y.sh waitdie  onesided $1 ycsb $cornum $outpath
#sleep $sleep_time
#./y.sh calvin  rpc $1 ycsb $cornum $outpath
#sleep $sleep_time
#./y.sh calvin  onesided $1 ycsb $cornum $outpath
#sleep $sleep_time



# tpcc $cornum
#sleep $sleep_time
#./y.sh occ  rpc $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh occ  onesided $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh sundial  rpc $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh sundial  onesided $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh mvcc  rpc $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh mvcc  onesided $1 tpcc $cornum $outpath
##sleep $sleep_time
##./y.sh mvcc  hybrid $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh nowait  rpc $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh nowait  onesided $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh waitdie  rpc $1 tpcc $cornum $outpath
#sleep $sleep_time
#./y.sh waitdie  onesided $1 tpcc $cornum $outpath
##sleep $sleep_time
##./y.sh calvin  rpc $1 tpcc $cornum $outpath
##sleep $sleep_time
##./y.sh calvin  onesided $1 tpcc $cornum $outpath
#sleep $sleep_time
