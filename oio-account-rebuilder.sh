#!/bin/bash
#set -x
usage() {
	echo "Usage: `basename $0` -r ip_redis_host:redis_port -n namespace -a account_name"
	echo "Example: `basename $0` -r 192.120.17.12:6051 -n OPENIO -a openio_account " ;
	exit
}

[ $# -lt 4 ] && usage

while getopts ":r:n:a:" opt; do
  case $opt in
    r)
      echo "-r was triggered, Parameter: $OPTARG" >&2
      REDIS_HOST=${OPTARG/:*/}
      REDIS_PORT=${OPTARG/*:/}
      if [[ $REDIS_HOST == "" ]]
          then
          echo "Missing  ip_redis_host"
	  exit 1
      fi
      if [[ $REDIS_PORT == "" ]]
	  then
	  echo "Missing  redis_port"
	  exit 1
      fi
      ;;
    n)
      echo "-n was triggered, Parameter: $OPTARG" >&2
      NAMESPACE=$OPTARG
      if [[ $NAMESPACE == "" ]]
          then
          echo "Missing namespace name"
          exit 1
      fi
      ;;
    a)
      echo "-a was triggered, Parameter: $OPTARG" >&2
      OIOACCOUNT=$OPTARG
      if [[ $OIOACCOUNT == "" ]]
	  then
          echo "Missing  account name"
	  exit 1
      fi
      ;;
     *)
	usage
	exit 0
      ;;
  esac
done

#Check if you are on the right server (master redis)
redis_bin=$(which redis-cli)
if [[ $(${redis_bin} -h $REDIS_HOST -p $REDIS_PORT role | grep -c master) -ne 1 ]]
	then
	echo "This script must be run on the master redis host"
	exit 1
fi

#Get account list
if [[ -z $OIOACCOUNT ]]
	then
	ACCOUNT_LIST=$(${redis_bin} -h $REDIS_HOST -p $REDIS_PORT  keys account:* | sed 's@.*account:\(.*\)@\1@' | tr "\n" " ")
else
	ACCOUNT_LIST=$OIOACCOUNT
fi

#Launch account repair
echo "You are about deleting all informations and refeed them for account: $ACCOUNT_LIST"
select yn in "Yes" "No"; do
    case $yn in
        Yes )
	    echo "Proceeding ...";
	    break
	;;
        No )
            exit
        ;;
    esac
done
for account in $ACCOUNT_LIST
do
	export OIO_NS=${NAMESPACE}
	export OIO_ACCOUNT=${account}
	echo "Status before reconstruction"
	openio account show ${account}
	echo
	#SAVE all container name
	${redis_bin} -h $REDIS_HOST -p $REDIS_PORT keys container:${account}:* > /tmp/container_list_${account}
	#Emptying all information about containers in account
	cat /tmp/container_list_${account} | while read line
	do
		echo "$line"
		${redis_bin} -p $REDIS_PORT -h $REDIS_HOST hset ${line} bytes  0 >/dev/null
		${redis_bin} -p $REDIS_PORT -h $REDIS_HOST hset ${line} objects 0 >/dev/null
	done
	echo "Zeroing information about objects and size in account"
	#Zeroing information about objects and size in account
	${redis_bin} -p $REDIS_PORT -h $REDIS_HOST hset account:${account} bytes  0 >/dev/null
	${redis_bin} -p $REDIS_PORT -h $REDIS_HOST hset account:${account} objects 0 >/dev/null
	echo "Status empty:"
	openio account show ${account}
	#Refeed account information (it may takes some times)
	echo "Retrieving information about all containers for account ${account} it may take a little time"
	cat /tmp/container_list_${account} | while read container
		do
		echo "Rebuilding info about ${container/*:/}" >&2
		echo "container touch ${container/*:/}"
	done | openio -v
	echo
done
echo "To see the final result it may take time depending on the number of container in account"
