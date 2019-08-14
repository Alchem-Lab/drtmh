cornum=$1
cp co2.ycsb co2

./y-ycsb.sh occ rpc 0 $cornum
./y-ycsb.sh occ onesided 0 $cornum
./y-ycsb.sh sundial rpc 0 $cornum
./y-ycsb.sh sundial onesided 0 $cornum
./y-ycsb.sh mvcc rpc 0 $cornum
./y-ycsb.sh mvcc onesided 0 $cornum
#./y-ycsb.sh mvcc hybrid 0 $cornum
./y-ycsb.sh nowait rpc 0 $cornum
./y-ycsb.sh nowait onesided 0 $cornum
./y-ycsb.sh waitdie rpc 0 $cornum
./y-ycsb.sh waitdie onesided 0 $cornum




#cp co2.bacl co2
#./y.sh occ  rpc 0 bank $cornum
#./y.sh occ  onesided 0 bank $cornum
#./y.sh sundial  rpc 0 bank $cornum
#./y.sh sundial  onesided 0 bank $cornum
#./y.sh mvcc  rpc 0 bank $cornum
#./y.sh mvcc  onesided 0 bank $cornum
##./y.sh mvcc  hybrid 0 bank $cornum
#./y.sh nowait  rpc 0 bank $cornum
#./y.sh nowait  onesided 0 bank $cornum
#./y.sh waitdie  rpc 0 bank $cornum
#./y.sh waitdie  onesided 0 bank $cornum
## tpcc
#./y.sh occ  rpc 0 tpcc $cornum
#./y.sh occ  onesided 0 tpcc $cornum
#./y.sh sundial  rpc 0 tpcc $cornum
#./y.sh sundial  onesided 0 tpcc $cornum
#./y.sh mvcc  rpc 0 tpcc $cornum
#./y.sh mvcc  onesided 0 tpcc $cornum
##./y.sh mvcc  hybrid 0 tpcc $cornum
#./y.sh nowait  rpc 0 tpcc $cornum
#./y.sh nowait  onesided 0 tpcc $cornum
#./y.sh waitdie  rpc 0 tpcc $cornum
#./y.sh waitdie  onesided 0 tpcc $cornum
#
