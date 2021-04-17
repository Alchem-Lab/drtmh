app="ycsb"
alg="mvcc"
cor=1
for dir in `ls out/hybrid/cor_$cor/$app/$alg`; do 
    echo "$dir " && awk -f tput.awk out/hybrid/cor_$cor/$app/$alg/$dir/drtmh-nocc${alg}-${app}-4-hybrid.log_0; 
done
