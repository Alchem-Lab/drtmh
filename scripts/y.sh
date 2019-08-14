function rand(){
	min=$1
	max=$(($2-$min+1))
	num=$(date +%s%N)
	echo $(($num%$max+$min))
}
protocol=$1
version=$2
id=$3
app=$4
cornum=$5
echo "protocol=$protocol version=$version id=$id app=$app cornum=$cornum"

if [ $id == "0" ];then
	cat co1 > config.xml
	rnd=$(rand 10 99)
	echo "  <port>88$rnd</port>" >> config.xml 
	cat co2 >> config.xml
fi
cat config.xml | grep "port"
cat config.xml | grep "ycsb"

date
./nocc$protocol-$version --bench $app --txn-flags 1  --verbose --config config.xml --id $id -t 10 -c $cornum -r 100 -p 4 1>out/cor$cornum/drtmh-nocc$protocol-$app-4-$version.log_$id 2>&1
