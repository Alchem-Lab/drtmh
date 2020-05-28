qp_num=1
if [ x$1 != "x" ];then
    qp_num=$1
fi
./make.sh occ $qp_num
./make.sh sundial $qp_num
./make.sh mvcc $qp_num
./make.sh waitdie $qp_num
./make.sh nowait $qp_num
