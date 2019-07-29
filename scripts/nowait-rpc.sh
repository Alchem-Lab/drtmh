function rand(){
	min=$1
	max=$(($2-$min+1))
	num=$(date +%s%N)
	echo $(($num%$max+$min))
}

if [ $1 == "0" ];then
	cat co1 > config.xml
	rnd=$(rand 10 99)
	echo "  <port>88$rnd</port>" >> config.xml 
	cat co2 >> config.xml
fi

cat config.xml | grep "port"

WL="bank"
if [ x"$2" != "x" ]; then
	WL=$2
fi
THREADS=1
if [ x"$3" != "x" ]; then
	THREADS=$3
fi
COROUTINES=1
if [ x"$4" != "x" ]; then
	COROUTINES=$4
fi

echo "running $WL with $THREADS*$COROUTINES..."
echo "./noccnowait --bench $WL --txn-flags 1  --verbose --config config.xml --id $1 -t $THREADS -c $COROUTINES -r 100 -p 2 1>tmp/drtmh-noccnowait-$WL-2-RPC.log_$1 2>&1"

./noccnowait --bench $WL --txn-flags 1  --verbose --config config.xml --id $1 -t $THREADS -c $COROUTINES -r 100 -p 2 1>tmp/drtmh-noccnowait-$WL-2-RPC.log_$1 2>&1
