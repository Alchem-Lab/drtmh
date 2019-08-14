cornum=$2

sleep 3
./y-ycsb.sh occ rpc $1 $cornum
sleep 3
./y-ycsb.sh occ onesided $1 $cornum
sleep 3
./y-ycsb.sh sundial rpc $1 $cornum
sleep 3
./y-ycsb.sh sundial onesided $1 $cornum
sleep 3
./y-ycsb.sh mvcc rpc $1 $cornum
sleep 3
./y-ycsb.sh mvcc onesided $1 $cornum
sleep 3
#./y-ycsb.sh mvcc hybrid $1 $cornum
#sleep 3
./y-ycsb.sh nowait rpc $1 $cornum
sleep 3
./y-ycsb.sh nowait onesided $1 $cornum
sleep 3
./y-ycsb.sh waitdie rpc $1 $cornum
sleep 3
./y-ycsb.sh waitdie onesided $1 $cornum
sleep 3




#./y.sh occ  rpc $1 bank $cornum
#sleep 3
#./y.sh occ  onesided $1 bank $cornum
#sleep 3
#./y.sh sundial  rpc $1 bank $cornum
#sleep 3
#./y.sh sundial  onesided $1 bank $cornum
#sleep 3
#./y.sh mvcc  rpc $1 bank $cornum
#sleep 3
#./y.sh mvcc  onesided $1 bank $cornum
#sleep 3
##./y.sh mvcc  hybrid $1 bank $cornum
##sleep 3
#./y.sh nowait  rpc $1 bank $cornum
#sleep 3
#./y.sh nowait  onesided $1 bank $cornum
#sleep 3
#./y.sh waitdie  rpc $1 bank $cornum
#sleep 3
#./y.sh waitdie  onesided $1 bank $cornum
#sleep 3
### tpcc $cornum
#./y.sh occ  rpc $1 tpcc $cornum
#sleep 3
#./y.sh occ  onesided $1 tpcc $cornum
#sleep 3
#./y.sh sundial  rpc $1 tpcc $cornum
#sleep 3
#./y.sh sundial  onesided $1 tpcc $cornum
#sleep 3
#./y.sh mvcc  rpc $1 tpcc $cornum
#sleep 3
#./y.sh mvcc  onesided $1 tpcc $cornum
#sleep 3
##./y.sh mvcc  hybrid $1 tpcc $cornum
##sleep 3
#./y.sh nowait  rpc $1 tpcc $cornum
#sleep 3
#./y.sh nowait  onesided $1 tpcc $cornum
#sleep 3
#./y.sh waitdie  rpc $1 tpcc $cornum
#sleep 3
#./y.sh waitdie  onesided $1 tpcc $cornum
#sleep 3
#
