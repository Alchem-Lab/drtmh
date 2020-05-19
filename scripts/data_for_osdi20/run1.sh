cornum=$2
outpath=$3

mkdir -p $outpath

# bank
sleep 3
./y.sh occ  rpc $1 bank $cornum $outpath
sleep 3
./y.sh occ  onesided $1 bank $cornum $outpath
sleep 3
./y.sh sundial  rpc $1 bank $cornum $outpath
sleep 3
./y.sh sundial  onesided $1 bank $cornum $outpath
sleep 3
./y.sh mvcc  rpc $1 bank $cornum $outpath
sleep 3
./y.sh mvcc  onesided $1 bank $cornum $outpath
sleep 3
./y.sh nowait  rpc $1 bank $cornum $outpath
sleep 3
./y.sh nowait  onesided $1 bank $cornum $outpath
sleep 3
./y.sh waitdie  rpc $1 bank $cornum $outpath
sleep 3
./y.sh waitdie  onesided $1 bank $cornum $outpath
sleep 3
#./y.sh calvin  rpc $1 bank $cornum $outpath
#sleep 3
#./y.sh calvin  onesided $1 bank $cornum $outpath
#sleep 3


# ycsb
sleep 3
./y.sh occ  rpc $1 ycsb $cornum $outpath
sleep 3
./y.sh occ  onesided $1 ycsb $cornum $outpath
sleep 3
./y.sh sundial  rpc $1 ycsb $cornum $outpath
sleep 3
./y.sh sundial  onesided $1 ycsb $cornum $outpath
sleep 3
./y.sh mvcc  rpc $1 ycsb $cornum $outpath
sleep 3
./y.sh mvcc  onesided $1 ycsb $cornum $outpath
sleep 3
./y.sh nowait  rpc $1 ycsb $cornum $outpath
sleep 3
./y.sh nowait  onesided $1 ycsb $cornum $outpath
sleep 3
./y.sh waitdie  rpc $1 ycsb $cornum $outpath
sleep 3
./y.sh waitdie  onesided $1 ycsb $cornum $outpath
sleep 3
#./y.sh calvin  rpc $1 ycsb $cornum $outpath
#sleep 3
#./y.sh calvin  onesided $1 ycsb $cornum $outpath
#sleep 3



### tpcc $cornum
#./y.sh occ  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh occ  onesided $1 tpcc $cornum $outpath
#sleep 3
#./y.sh sundial  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh sundial  onesided $1 tpcc $cornum $outpath
#sleep 3
#./y.sh mvcc  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh mvcc  onesided $1 tpcc $cornum $outpath
#sleep 3
##./y.sh mvcc  hybrid $1 tpcc $cornum $outpath
##sleep 3
#./y.sh nowait  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh nowait  onesided $1 tpcc $cornum $outpath
#sleep 3
#./y.sh waitdie  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh waitdie  onesided $1 tpcc $cornum $outpath
#sleep 3
#./y.sh calvin  rpc $1 tpcc $cornum $outpath
#sleep 3
#./y.sh calvin  onesided $1 tpcc $cornum $outpath
#sleep 3
#
