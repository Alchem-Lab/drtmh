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

./noccsundial --bench bank --txn-flags 1  --verbose --config config.xml --id $1 -t 8 -c 10 -r 100 -p 2 1>out/drtmh-noccsundial-bank-2-RPC.log_$1 2>&1
