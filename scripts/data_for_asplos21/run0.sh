cornum=$1
outpath=$2

mkdir -p $outpath

# bank
#cp co2.bank co2
#./y.sh occ  rpc 0 bank $cornum $outpath
#./y.sh occ  onesided 0 bank $cornum $outpath
#./y.sh sundial  rpc 0 bank $cornum $outpath
#./y.sh sundial  onesided 0 bank $cornum $outpath
#./y.sh mvcc  rpc 0 bank $cornum $outpath
#./y.sh mvcc  onesided 0 bank $cornum $outpath
#./y.sh nowait  rpc 0 bank $cornum $outpath
#./y.sh nowait  onesided 0 bank $cornum $outpath
#./y.sh waitdie  rpc 0 bank $cornum $outpath
#./y.sh waitdie  onesided 0 bank $cornum $outpath
##./y.sh calvin  rpc 0 bank $cornum $outpath
##./y.sh calvin  onesided 0 bank $cornum $outpath

# ycsb
cp co2.ycsb co2
./y.sh occ  rpc 0 ycsb $cornum $outpath
./y.sh occ  onesided 0 ycsb $cornum $outpath
./y.sh sundial  rpc 0 ycsb $cornum $outpath
./y.sh sundial  onesided 0 ycsb $cornum $outpath
./y.sh mvcc  rpc 0 ycsb $cornum $outpath
./y.sh mvcc  onesided 0 ycsb $cornum $outpath
./y.sh nowait  rpc 0 ycsb $cornum $outpath
./y.sh nowait  onesided 0 ycsb $cornum $outpath
./y.sh waitdie  rpc 0 ycsb $cornum $outpath
./y.sh waitdie  onesided 0 ycsb $cornum $outpath
#./y.sh calvin  rpc 0 ycsb $cornum $outpath
#./y.sh calvin  onesided 0 ycsb $cornum $outpath

## tpcc
#./y.sh occ  rpc 0 tpcc $cornum $outpath
#./y.sh occ  onesided 0 tpcc $cornum $outpath
#./y.sh sundial  rpc 0 tpcc $cornum $outpath
#./y.sh sundial  onesided 0 tpcc $cornum $outpath
#./y.sh mvcc  rpc 0 tpcc $cornum $outpath
#./y.sh mvcc  onesided 0 tpcc $cornum $outpath
##./y.sh mvcc  hybrid 0 tpcc $cornum $outpath
#./y.sh nowait  rpc 0 tpcc $cornum $outpath
#./y.sh nowait  onesided 0 tpcc $cornum $outpath
#./y.sh waitdie  rpc 0 tpcc $cornum $outpath
#./y.sh waitdie  onesided 0 tpcc $cornum $outpath
##./y.sh calvin  rpc 0 tpcc $cornum $outpath
##./y.sh calvin  onesided 0 tpcc $cornum $outpath
