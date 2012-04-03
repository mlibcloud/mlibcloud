
. mlist.sh

for ip in $IPS
do
	echo $ip
	ssh root@$ip "pgrep python; cat /root/mlibcloud/test-checkpoint"
	echo ""
done
