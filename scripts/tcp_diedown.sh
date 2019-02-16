for mac in $@; do
	echo "netstat $mac."
	ssh $mac "netstat -ntp | grep 8888"
done

echo "netstat `hostname`"
netstat -ntp | grep 8888
