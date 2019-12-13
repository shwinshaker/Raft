##################################################
# File  Name: free_port.sh
#     Author: shwin
# Creat Time: Sat Dec  7 12:34:42 2019
##################################################

#!/bin/bash

{
    read;
    while read line || [[ -n $line  ]]
    do
	port=$(echo $line | awk -F ':' '{print$3}')
	echo $port
	pids=$(lsof -ti:$port)
	echo $pids
	[[ ! -z $pids ]] && kill $pids
    done
} < config.txt
