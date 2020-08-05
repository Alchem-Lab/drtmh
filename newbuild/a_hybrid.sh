hybrid_code=0
if [ x$1 != "x" ];then
    hybrid_code=$1
fi
./make_hybrid.sh occ $hybrid_code
./make_hybrid.sh sundial $hybrid_code
./make_hybrid.sh mvcc $hybrid_code
./make_hybrid.sh waitdie $hybrid_code
./make_hybrid.sh nowait $hybrid_code
