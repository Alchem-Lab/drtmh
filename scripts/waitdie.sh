function rand(){
	min=$1
	max=$(($2-$min+1))
	num=$(date +%s%N)
	echo $(($num%$max+$min))
}

if [ $2 == "0" ];then
	cat co1 > config.xml
	rnd=$(rand 10 99)
	echo "  <port>88$rnd</port>" >> config.xml 
	cat co2 >> config.xml
fi

cat config.xml | grep "port"

./noccwaitdie-$1 --bench bank --txn-flags 1  --verbose --config config.xml --id $2 -t 8 -c 10 -r 100 -p 2 1>out/drtmh-noccwaitdie-bank-2-$1.log_$2 2>&1
