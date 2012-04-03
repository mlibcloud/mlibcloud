auto_ssh_copy_id () {
	expect -c "set timeout -1;
			spawn ssh-copy-id $2;
			expect {
				*(yes/no)* {send -- yes\r;exp_continue;}
				*assword:* {send -- $1\r;exp_continue;}
				eof        {exit 0;}
			}";
		}


. mlist.sh

for ip in $IPS
do
	auto_ssh_copy_id HPCgrid root@$ip 
done

